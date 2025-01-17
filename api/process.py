from contextlib import asynccontextmanager
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, Request, HTTPException
from fastapi.responses import Response, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import numpy as np
from io import BytesIO
import asyncio
import uuid
import time
import json
import os
from datetime import datetime
from upstash_redis import Redis
import base64

# Redis 连接配置
UPSTASH_REDIS_REST_URL = os.getenv('UPSTASH_REDIS_REST_URL')
UPSTASH_REDIS_REST_TOKEN = os.getenv('UPSTASH_REDIS_REST_TOKEN')

print("[DEBUG] ===== Redis 配置信息 =====")
print(f"[DEBUG] UPSTASH_REDIS_REST_URL: {UPSTASH_REDIS_REST_URL}")
print(f"[DEBUG] UPSTASH_REDIS_REST_TOKEN: {'***' + UPSTASH_REDIS_REST_TOKEN[-8:] if UPSTASH_REDIS_REST_TOKEN else 'None'}")

# 创建 Redis 客户端
redis = None

# 任务过期时间（秒）
TASK_EXPIRY = 3600  # 1小时

def get_redis_client():
    """获取 Redis 客户端实例"""
    global redis
    try:
        if not redis:
            if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
                print("[ERROR] Redis 配置未设置")
                raise Exception("请检查 UPSTASH_REDIS_REST_URL 和 UPSTASH_REDIS_REST_TOKEN 环境变量")
            
            print("[DEBUG] 正在初始化 Redis 客户端")
            redis = Redis(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)
            
            # 测试连接
            test_key = "test:init"
            test_value = "connection_test"
            redis.set(test_key, test_value)
            result = redis.get(test_key)
            redis.delete(test_key)
            
            if result != test_value:
                raise Exception("Redis 连接测试失败")
            
            print("[DEBUG] Redis 客户端初始化成功")
        return redis
    except Exception as e:
        print(f"[ERROR] Redis 客户端初始化失败: {str(e)}")
        raise

async def get_task_status(task_id: str) -> dict:
    """从Redis获取任务状态"""
    try:
        redis_client = get_redis_client()
        status = redis_client.get(f"task:{task_id}")
        if status:
            return json.loads(status)
        return None
    except Exception as e:
        print(f"[ERROR] 获取任务状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取任务状态失败: {str(e)}")

async def set_task_status(task_id: str, status: dict):
    """设置任务状态到Redis"""
    try:
        redis_client = get_redis_client()
        redis_client.setex(
            f"task:{task_id}",
            TASK_EXPIRY,
            json.dumps(status)
        )
    except Exception as e:
        print(f"[ERROR] 设置任务状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"设置任务状态失败: {str(e)}")

# 创建 FastAPI 应用实例
app = FastAPI()

# 配置静态文件
app.mount("/static", StaticFiles(directory="api/static"), name="static")

# 配置模板
templates = Jinja2Templates(directory="api/templates")

@app.on_event("startup")
async def startup_event():
    """应用程序启动时的事件处理"""
    global redis
    try:
        redis = await get_redis_connection()
        print("[DEBUG] 应用程序启动时 Redis 连接状态:", redis is not None)
    except Exception as e:
        print(f"[ERROR] 应用程序启动失败: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """应用程序关闭时的事件处理"""
    global redis
    if redis:
        print("[DEBUG] 正在关闭 Redis 连接")
        await redis.close()
        print("[DEBUG] Redis 连接已关闭")

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """返回主页"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.exception_handler(404)
async def not_found_handler(request: Request, exc: HTTPException):
    """处理404错误"""
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "error": "页面未找到"},
        status_code=404
    ) 

@app.post("/api/process")
async def handle_upload(background_tasks: BackgroundTasks, order_file: UploadFile = File(...), schedule_file: UploadFile = File(...)):
    """处理文件上传"""
    try:
        print(f"[DEBUG] 开始处理文件上传...")
        print(f"[DEBUG] Redis 连接状态: {redis is not None}")
        print(f"[DEBUG] Redis 配置: UPSTASH_REDIS_REST_URL 已设置: {bool(UPSTASH_REDIS_REST_URL)}")
        print(f"[DEBUG] 订单文件名: {order_file.filename}")
        print(f"[DEBUG] 排班表文件名: {schedule_file.filename}")
        
        # 验证文件扩展名
        if not order_file.filename.endswith('.xlsx') or not schedule_file.filename.endswith('.xlsx'):
            print("[ERROR] 文件格式错误：非 .xlsx 格式")
            return JSONResponse(
                status_code=400,
                content={"error": "文件格式错误：请上传 .xlsx 格式的文件"}
            )

        # 读取文件内容
        try:
            print("[DEBUG] 正在读取订单文件...")
            order_data = await order_file.read()
            print(f"[DEBUG] 订单文件大小: {len(order_data)} bytes")
            
            print("[DEBUG] 正在读取排班表文件...")
            schedule_data = await schedule_file.read()
            print(f"[DEBUG] 排班表文件大小: {len(schedule_data)} bytes")
        except Exception as e:
            print(f"[ERROR] 文件读取失败: {str(e)}")
            return JSONResponse(
                status_code=400,
                content={"error": f"文件读取失败：{str(e)}"}
            )

        # 验证文件大小
        MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
        if len(order_data) > MAX_FILE_SIZE or len(schedule_data) > MAX_FILE_SIZE:
            print("[ERROR] 文件过大")
            return JSONResponse(
                status_code=400,
                content={"error": "文件过大：请确保每个文件小于100MB"}
            )

        try:
            # 创建任务ID并初始化状态
            task_id = str(uuid.uuid4())
            print(f"[DEBUG] 创建任务ID: {task_id}")
            
            await set_task_status(task_id, {
                "status": "pending",
                "progress": 0,
                "message": "正在准备处理...",
                "start_time": time.time()
            })
            print("[DEBUG] 任务状态已初始化")
            
            # 启动后台任务
            background_tasks.add_task(process_data_in_background, task_id, order_data, schedule_data)
            print("[DEBUG] 后台任务已启动")

            # 返回任务ID
            return JSONResponse(
                content={
                    "task_id": task_id,
                    "message": "文件已接收，正在处理中"
                }
            )

        except Exception as e:
            print(f"[ERROR] 任务创建失败: {str(e)}")
            if redis:
                print(f"[DEBUG] Redis 连接正常")
            else:
                print(f"[ERROR] Redis 未连接")
            return JSONResponse(
                status_code=500,
                content={"error": f"任务创建失败：{str(e)}"}
            )

    except Exception as e:
        print(f"[ERROR] 系统错误: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"系统错误：{str(e)}"}
        )

@app.get("/api/status/{task_id}")
async def get_task_status_endpoint(task_id: str):
    """获取任务处理状态"""
    try:
        print(f"正在获取任务状态: {task_id}")
        
        status = await get_task_status(task_id)
        if not status:
            print(f"任务不存在: {task_id}")
            return JSONResponse(
                status_code=404,
                content={"error": "任务不存在"}
            )
        
        print(f"任务状态: {status}")
        
        # 根据任务状态返回不同的响应
        if status["status"] == "completed":
            # 如果任务完成且有结果，返回base64编码的Excel文件
            result = status.get("result")
            if result:
                print(f"返回任务结果文件: {task_id}")
                return JSONResponse(content={
                    "status": "completed",
                    "progress": 100,
                    "message": "处理完成",
                    "result": result,
                    "filename": "processed_result.xlsx"
                })
            # 如果没有结果，返回完成状态
            print(f"任务完成但无结果: {task_id}")
            return JSONResponse(content={
                "status": "completed",
                "progress": 100,
                "message": "处理完成"
            })
            
        elif status["status"] == "failed":
            # 任务失败时返回错误信息
            print(f"任务失败: {task_id}, 错误: {status.get('message')}")
            return JSONResponse(
                status_code=500,
                content={
                    "status": "failed",
                    "message": status.get("message", "处理失败"),
                    "progress": 0
                }
            )
        
        else:
            # 处理中的任务返回进度信息
            print(f"任务处理中: {task_id}, 进度: {status.get('progress')}%")
            return JSONResponse(content={
                "status": status["status"],
                "progress": status.get("progress", 0),
                "message": status.get("message", "正在处理...")
            })
            
    except Exception as e:
        print(f"获取任务状态时出错: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"获取任务状态失败：{str(e)}",
                "progress": 0
            }
        )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """全局异常处理器"""
    error_msg = str(exc)
    print(f"全局异常: {error_msg}")
    return JSONResponse(
        status_code=500,
        content={
            "error": error_msg,
            "path": request.url.path
        }
    ) 

async def process_data_in_background(task_id: str, order_data: bytes, schedule_data: bytes):
    """后台处理数据的异步函数"""
    try:
        # 更新任务状态
        await set_task_status(task_id, {
            "status": "processing",
            "progress": 10,
            "message": "正在读取数据...",
            "start_time": time.time()
        })
        
        # 处理数据
        result = await process_excel_async(order_data, schedule_data, task_id)
        
        # 将结果转换为base64
        result_base64 = base64.b64encode(result).decode('utf-8')
        
        # 更新任务状态为完成
        await set_task_status(task_id, {
            "status": "completed",
            "progress": 100,
            "message": "处理完成",
            "result": result_base64
        })
    except Exception as e:
        # 更新任务状态为失败
        await set_task_status(task_id, {
            "status": "failed",
            "message": str(e)
        })

async def process_excel_async(order_data: bytes, schedule_data: bytes, task_id: str):
    """异步处理 Excel 数据的核心逻辑"""
    try:
        # 更新状态：开始处理
        await set_task_status(task_id, {
            "status": "processing",
            "progress": 20,
            "message": "正在验证数据格式..."
        })

        try:
            # 读取订单数据
            df = pd.read_excel(BytesIO(order_data))
        except Exception as e:
            raise Exception(f"读取订单数据失败: {str(e)}")

        # 验证必要的列是否存在
        required_order_columns = ['主订单编号', '子订单编号', '商品ID', '选购商品', '流量来源', 
                                '流量体裁', '取消原因', '订单状态', '订单应付金额', 
                                '订单提交日期', '订单提交时间']
        required_schedule_columns = ['日期', '上播时间', '下播时间', '主播姓名', '场控姓名', '时段消耗']

        try:
            # 验证订单数据列
            missing_columns = [col for col in required_order_columns if col not in df.columns]
            if missing_columns:
                raise Exception(f"订单数据缺少必要的列：{', '.join(missing_columns)}")
        except Exception as e:
            raise Exception(f"验证订单数据列失败: {str(e)}")

        # 更新进度
        await set_task_status(task_id, {
            "status": "processing",
            "progress": 40,
            "message": "正在处理订单数据..."
        })

        # 使用 chunk 处理大数据
        chunk_size = 10000
        total_chunks = len(df) // chunk_size + 1
        df_filtered_list = []
        
        for i in range(total_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(df))
            chunk = df.iloc[start_idx:end_idx].copy()
            
            # 转为字符串以防后续合并或过滤问题
            chunk[['主订单编号', '子订单编号', '商品ID']] = chunk[['主订单编号', '子订单编号', '商品ID']].astype(str)
            
            # 应用过滤条件
            keywords = ['SSS', 'DB', 'TZDN', 'DF', 'SP', 'sp', 'SC', 'sc', 'spcy']
            chunk_filtered = chunk[~chunk['选购商品'].apply(lambda x: any(kw in str(x) for kw in keywords))]
            chunk_filtered = chunk_filtered[~chunk_filtered['流量来源'].str.contains('精选联盟', na=False)]
            
            # 根据"流量体裁"筛选
            mask_1 = (chunk_filtered['流量体裁'] == '其他') & (chunk_filtered['订单应付金额'] != 0)
            mask_2 = chunk_filtered['流量体裁'] == '直播'
            mask_3 = chunk_filtered['流量体裁'] == '数据将于第二天更新'
            chunk_filtered = chunk_filtered[mask_1 | mask_2 | mask_3]
            
            # 筛选"取消原因"列为空
            chunk_filtered = chunk_filtered[chunk_filtered['取消原因'].isna()]
            
            df_filtered_list.append(chunk_filtered)
            
            # 更新进度
            progress = 40 + (i + 1) * 20 // total_chunks
            await set_task_status(task_id, {
                "status": "processing",
                "progress": progress,
                "message": f"正在处理订单数据... ({i + 1}/{total_chunks})"
            })
            
            await asyncio.sleep(0)

        df_filtered = pd.concat(df_filtered_list, ignore_index=True)
        
        if df_filtered.empty:
            raise Exception("过滤后没有任何数据，请检查过滤条件是否过于严格或数据是否符合要求")

        # 更新进度
        await set_task_status(task_id, {
            "status": "processing",
            "progress": 60,
            "message": "正在处理排班表..."
        })

        # ========== 第 2 步：读取并验证排班表 ==========
        try:
            # 读取排班表
            df_schedule = pd.read_excel(BytesIO(schedule_data))
        except Exception as e:
            raise Exception(f"读取排班表失败: {str(e)}")

        try:
            # 验证排班表列
            missing_columns = [col for col in required_schedule_columns if col not in df_schedule.columns]
            if missing_columns:
                raise Exception(f"排班表缺少必要的列：{', '.join(missing_columns)}")
        except Exception as e:
            raise Exception(f"验证排班表列失败: {str(e)}")

        # 更新进度
        await set_task_status(task_id, {
            "status": "processing",
            "progress": 70,
            "message": "正在处理日期时间数据..."
        })

        # ========== 第 3 步：统一转换日期/时间类型 ==========
        # 处理日期时间转换
        try:
            df_filtered['订单提交日期'] = pd.to_datetime(df_filtered['订单提交日期'], errors='coerce').dt.date
            df_schedule['日期'] = pd.to_datetime(df_schedule['日期'], errors='coerce').dt.date
        except Exception as e:
            raise Exception(f"日期转换失败: {str(e)}")
        
        for df, time_cols in [
            (df_filtered, ['订单提交时间']),
            (df_schedule, ['上播时间', '下播时间'])
        ]:
            for col in time_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(
                        df[col].astype(str).str.strip(),
                        format='%H:%M:%S',
                        errors='coerce'
                    ).dt.time

        # 检查日期时间转换后的空值
        date_time_errors = []
        if df_filtered['订单提交日期'].isna().any():
            date_time_errors.append("订单数据中存在无效的日期格式")
        if df_filtered['订单提交时间'].isna().any():
            date_time_errors.append("订单数据中存在无效的时间格式")
        if df_schedule['日期'].isna().any():
            date_time_errors.append("排班表中存在无效的日期格式")
        if df_schedule['上播时间'].isna().any():
            date_time_errors.append("排班表中存在无效的上播时间格式")
        if df_schedule['下播时间'].isna().any():
            date_time_errors.append("排班表中存在无效的下播时间格式")

        if date_time_errors:
            raise Exception("日期时间格式错误：" + "；".join(date_time_errors))

        # 更新进度
        await set_task_status(task_id, {
            "status": "processing",
            "progress": 80,
            "message": "正在计算统计数据..."
        })

        # ========== 第 4 步：匹配并统计"订单应付金额" ==========
        df_schedule['GMV'] = 0.0
        df_schedule['退货GMV'] = 0.0
        df_schedule['GSV'] = 0.0

        # 按日期分组处理，减少内存使用
        for date in df_schedule['日期'].unique():
            schedule_mask = df_schedule['日期'] == date
            order_mask = df_filtered['订单提交日期'] == date
            
            schedule_day = df_schedule[schedule_mask]
            orders_day = df_filtered[order_mask]
            
            if orders_day.empty:
                continue
                
            for i, row in schedule_day.iterrows():
                start_time = row['上播时间']
                end_time = row['下播时间']
                
                if pd.isna(start_time) or pd.isna(end_time):
                    continue
                
                mask_time = (
                    (orders_day['订单提交时间'] >= start_time) &
                    (orders_day['订单提交时间'] <= end_time)
                )
                
                # GMV
                mask_status_GMV = orders_day['订单状态'].isin(['已发货', '已完成', '已关闭', '待发货'])
                matched_df_GMV = orders_day[mask_time & mask_status_GMV]
                df_schedule.at[i, 'GMV'] = matched_df_GMV['订单应付金额'].sum()
                
                # 退货GMV
                mask_status_refund = (orders_day['订单状态'] == '已关闭')
                matched_df_refund = orders_day[mask_time & mask_status_refund]
                df_schedule.at[i, '退货GMV'] = matched_df_refund['订单应付金额'].sum()
                
                # GSV
                mask_status_GSV = orders_day['订单状态'].isin(['已发货', '已完成', '待发货'])
                matched_df_GSV = orders_day[mask_time & mask_status_GSV]
                df_schedule.at[i, 'GSV'] = matched_df_GSV['订单应付金额'].sum()
            
            # 让出控制权
            await asyncio.sleep(0)

        # 更新进度
        await set_task_status(task_id, {
            "status": "processing",
            "progress": 90,
            "message": "正在生成汇总报表..."
        })

        # ========== 第 5-6 步：汇总统计 ==========
        cols_to_sum = ['GMV', '退货GMV', 'GSV', '时段消耗']
        
        # 主播汇总
        if '主播姓名' in df_schedule.columns:
            df_anchor_sum = df_schedule.groupby('主播姓名', as_index=False)[cols_to_sum].sum()
            df_anchor_sum.columns = ['主播姓名', '主播GMV总和', '主播退货GMV总和', '主播GSV总和', '总消耗']
        else:
            df_anchor_sum = pd.DataFrame()
            
        # 场控汇总
        if '场控姓名' in df_schedule.columns:
            df_ck_sum = df_schedule.groupby('场控姓名', as_index=False)[cols_to_sum].sum()
            df_ck_sum.columns = ['场控姓名', '场控GMV总和', '场控退货GMV总和', '场控GSV总和', '总消耗']
        else:
            df_ck_sum = pd.DataFrame()

        # ========== 第 7 步：写入结果 ==========
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_filtered.to_excel(writer, sheet_name='主播、场控业绩筛选源表', index=False)
            df_schedule.to_excel(writer, sheet_name='主播、场控排班', index=False)
            if not df_anchor_sum.empty:
                df_anchor_sum.to_excel(writer, sheet_name='主播月总业绩汇总', index=False)
            if not df_ck_sum.empty:
                df_ck_sum.to_excel(writer, sheet_name='场控月总业绩汇总', index=False)

        output.seek(0)
        return output.getvalue()

    except Exception as e:
        error_msg = f"数据处理失败: {str(e)}"
        print(error_msg)  # 打印错误日志
        raise Exception(error_msg) 

@app.get("/api/test-redis")
async def test_redis_connection():
    """测试 Redis 连接"""
    try:
        redis_client = get_redis_client()
        
        # 测试基本操作
        test_key = "test:connection"
        test_value = "test_value"
        
        # 写入测试
        redis_client.set(test_key, test_value)
        print("[DEBUG] Redis 写入测试成功")
        
        # 读取测试
        result = redis_client.get(test_key)
        print(f"[DEBUG] Redis 读取测试结果: {result}")
        
        # 删除测试
        redis_client.delete(test_key)
        print("[DEBUG] Redis 删除测试成功")
        
        if result == test_value:
            return JSONResponse(
                content={
                    "status": "success",
                    "message": "Redis 连接测试成功",
                    "details": {
                        "write": True,
                        "read": True,
                        "delete": True
                    }
                }
            )
        else:
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": f"Redis 读取值不匹配: 期望 '{test_value}', 实际 '{result}'"
                }
            )
    except Exception as e:
        print(f"[ERROR] Redis 连接测试失败: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Redis 连接测试失败: {str(e)}"
            }
        ) 