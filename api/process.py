import os
import uuid
import time
import json
import logging
import traceback
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from fastapi import FastAPI, File, UploadFile, Request, WebSocket, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from redis import Redis
import pandas as pd
import numpy as np
import io
import base64
import asyncio
import tempfile
import gc
import openpyxl
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 内存中存储日志
log_buffer = []
log_id_counter = 0

class BufferLogHandler(logging.Handler):
    def emit(self, record):
        global log_id_counter
        log_id_counter += 1
        log_entry = {
            "id": log_id_counter,
            "level": record.levelname.lower(),
            "message": self.format(record),
            "timestamp": time.time()
        }
        log_buffer.append(log_entry)
        # 保持最多1000条日志
        if len(log_buffer) > 1000:
            log_buffer.pop(0)

# 添加自定义日志处理器
logger.addHandler(BufferLogHandler())

# Redis 配置
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

# 创建 FastAPI 应用
app = FastAPI()

# WebSocket 连接管理
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket 连接建立，当前连接数: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket 连接断开，当前连接数: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"广播消息失败: {str(e)}")
                await self.disconnect(connection)

manager = ConnectionManager()

class WebSocketLogHandler(logging.Handler):
    def emit(self, record):
        try:
            log_entry = {
                "level": record.levelname.lower(),
                "message": self.format(record)
            }
            # 使用同步方式发送日志
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(manager.broadcast(json.dumps(log_entry)))
            else:
                loop.run_until_complete(manager.broadcast(json.dumps(log_entry)))
        except Exception as e:
            print(f"WebSocket 日志发送失败: {str(e)}")

# 添加 WebSocket 日志处理器
websocket_handler = WebSocketLogHandler()
logger.addHandler(websocket_handler)

# 配置模板
templates = Jinja2Templates(directory="api/templates")

# 添加静态文件服务
app.mount("/static", StaticFiles(directory="api/static"), name="static")

# 添加 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 任务状态常量
TASK_STATUS_PENDING = "pending"
TASK_STATUS_PROCESSING = "processing"
TASK_STATUS_COMPLETED = "completed"
TASK_STATUS_FAILED = "failed"

def get_redis_client() -> Optional[Redis]:
    """获取 Redis 客户端实例"""
    try:
        if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
            raise ValueError("Redis 配置缺失")
            
        # 从 URL 中提取主机名
        from urllib.parse import urlparse
        parsed_url = urlparse(UPSTASH_REDIS_REST_URL)
        hostname = parsed_url.hostname
        
        redis_client = Redis(
            host=hostname,  # 使用解析后的主机名
            port=parsed_url.port or 6379,  # 使用 URL 中的端口或默认端口
            password=UPSTASH_REDIS_REST_TOKEN,
            ssl=True,  # 启用 SSL
            decode_responses=True
        )
        
        # 测试连接
        redis_client.set("test_key", "test_value")
        test_value = redis_client.get("test_key")
        if test_value != "test_value":
            raise ValueError("Redis 连接测试失败")
            
        return redis_client
    except Exception as e:
        logger.error(f"Redis 客户端初始化失败: {str(e)}")
        return None

async def get_task_status(task_id: str) -> Optional[Dict[str, Any]]:
    """获取任务状态"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            raise Exception("Redis 客户端初始化失败")
            
        status_key = f"task:{task_id}:status"
        status_data = redis_client.get(status_key)
        
        if not status_data:
            return None
            
        return json.loads(status_data)
    except Exception as e:
        logger.error(f"获取任务状态失败: {str(e)}")
        return None

async def set_task_status(task_id: str, status: Dict[str, Any]) -> bool:
    """设置任务状态"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            raise Exception("Redis 客户端初始化失败")
            
        status_key = f"task:{task_id}:status"
        redis_client.set(status_key, json.dumps(status))
        return True
    except Exception as e:
        logger.error(f"设置任务状态失败: {str(e)}")
        return False

@app.post("/api/process")
async def process_files(
    request: Request,
    order_file: UploadFile = File(...),
    schedule_file: UploadFile = File(...),
):
    """处理上传的文件"""
    try:
        # 1. 首先通过 Content-Length 快速判断总体大小
        content_length = request.headers.get('content-length')
        if content_length and int(content_length) > 200 * 1024 * 1024:  # 200MB (两个文件总和)
            raise HTTPException(status_code=413, detail="上传文件总大小超过限制")
            
        # 2. 验证文件扩展名
        if not order_file.filename.endswith('.xlsx'):
            raise HTTPException(status_code=400, detail="订单文件必须是 Excel 文件(.xlsx)")
        if not schedule_file.filename.endswith('.xlsx'):
            raise HTTPException(status_code=400, detail="排班表必须是 Excel 文件(.xlsx)")
            
        # 3. 验证单个文件大小
        max_size = 100 * 1024 * 1024  # 100MB
        
        # 验证订单文件大小
        order_file.file.seek(0, 2)  # 移动到文件末尾
        order_size = order_file.file.tell()  # 获取文件大小
        order_file.file.seek(0)  # 回到文件开头
        
        if order_size > max_size:
            raise HTTPException(status_code=413, detail="订单文件大小超过限制")
            
        # 验证排班表大小
        schedule_file.file.seek(0, 2)
        schedule_size = schedule_file.file.tell()
        schedule_file.file.seek(0)
        
        if schedule_size > max_size:
            raise HTTPException(status_code=413, detail="排班表文件大小超过限制")
            
        # 4. 生成任务ID
        task_id = str(uuid.uuid4())
        logger.info(f"开始处理任务 {task_id}")
        
        # 5. 保存文件到临时目录
        order_temp = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        schedule_temp = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        
        try:
            # 6. 分块读取并写入文件
            chunk_size = 1024 * 1024  # 每次读取 1MB
            
            # 复制订单文件
            while True:
                chunk = await order_file.read(chunk_size)
                if not chunk:
                    break
                order_temp.write(chunk)
                
            # 复制排班表文件
            while True:
                chunk = await schedule_file.read(chunk_size)
                if not chunk:
                    break
                schedule_temp.write(chunk)
                
            # 关闭文件
            order_temp.close()
            schedule_temp.close()
            
            logger.info(f"文件上传完成: order_file={order_file.filename}({order_size/1024/1024:.2f}MB), schedule_file={schedule_file.filename}({schedule_size/1024/1024:.2f}MB)")
            
            # 7. 初始化任务状态
            await set_task_status(task_id, {
                'status': 'processing',
                'progress': 0,
                'message': '开始处理...',
                'start_time': datetime.now().isoformat()  # 使用导入的 datetime 类
            })
            
            # 8. 启动后台处理
            asyncio.create_task(process_data_in_background(
                task_id=task_id,
                order_file_path=order_temp.name,
                schedule_file_path=schedule_temp.name
            ))
            
            # 9. 立即返回任务ID
            return {"task_id": task_id}
            
        except Exception as e:
            # 清理临时文件
            try:
                os.unlink(order_temp.name)
                os.unlink(schedule_temp.name)
            except:
                pass
            raise HTTPException(status_code=500, detail=f"文件处理失败: {str(e)}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"文件上传失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"文件上传失败: {str(e)}")

@app.get("/api/status/{task_id}")
async def get_task_status_endpoint(task_id: str):
    """获取任务处理状态"""
    try:
        # 1. 获取 Redis 客户端
        redis_client = get_redis_client()
        if not redis_client:
            logger.error("Redis 客户端初始化失败")
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": "Redis 连接失败",
                    "progress": 0
                }
            )
            
        # 2. 获取任务状态
        status_key = f"task:{task_id}:status"
        try:
            status_data = redis_client.get(status_key)
        except Exception as e:
            logger.error(f"从 Redis 获取状态失败: {str(e)}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": f"获取任务状态失败: {str(e)}",
                    "progress": 0
                }
            )
        
        # 3. 检查任务是否存在
        if not status_data:
            logger.warning(f"任务不存在: {task_id}")
            return JSONResponse(
                status_code=404,
                content={
                    "status": "not_found",
                    "message": "任务不存在",
                    "progress": 0
                }
            )
            
        # 4. 解析状态数据
        try:
            status = json.loads(status_data)
        except json.JSONDecodeError as e:
            logger.error(f"状态数据解析失败: {str(e)}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": "状态数据格式错误",
                    "progress": 0
                }
            )
        
        # 5. 根据任务状态返回不同的响应
        if status["status"] == "completed":
            result = status.get("result")
            if result:
                return JSONResponse(content={
                    "status": "completed",
                    "progress": 100,
                    "message": "处理完成",
                    "result": result
                })
            return JSONResponse(content={
                "status": "completed",
                "progress": 100,
                "message": "处理完成"
            })
            
        elif status["status"] == "failed":
            return JSONResponse(content={
                "status": "failed",
                "message": status.get("message", "处理失败"),
                "progress": 0
            })
            
        else:
            return JSONResponse(content={
                "status": status["status"],
                "progress": status.get("progress", 0),
                "message": status.get("message", "正在处理...")
            })
            
    except Exception as e:
        logger.error(f"获取任务状态时发生未知错误: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"系统错误: {str(e)}",
                "progress": 0
            }
        )

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """根路由处理"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.websocket("/api/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket 连接处理"""
    try:
        await manager.connect(websocket)
        while True:
            try:
                # 保持连接活跃
                data = await websocket.receive_text()
                # 可以在这里处理接收到的消息
            except Exception as e:
                logger.error(f"WebSocket 接收消息失败: {str(e)}")
                break
    except Exception as e:
        logger.error(f"WebSocket 连接错误: {str(e)}")
    finally:
        manager.disconnect(websocket)

@app.get("/api/logs")
async def get_logs(since: int = 0):
    """获取日志"""
    try:
        # 返回ID大于since的所有日志
        logs = [log for log in log_buffer if log["id"] > since]
        return JSONResponse(content=logs)
    except Exception as e:
        logger.error(f"获取日志失败: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"获取日志失败：{str(e)}"}
        )

def read_excel_with_header_fix(file_path: str, sheet_name: str = None) -> pd.DataFrame:
    """
    读取 Excel 文件并自动处理表头问题
    """
    try:
        # 首先尝试读取第一行作为表头
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
        
        # 检查第一行是否与列名重复
        first_row_values = df.iloc[0].astype(str).tolist()
        col_names = df.columns.astype(str).tolist()
        
        # 计算重复程度
        same_count = sum(v == c for v, c in zip(first_row_values, col_names))
        if same_count >= len(col_names) - 1:
            logger.info("检测到重复的表头行，将自动删除")
            df.drop(index=df.index[0], inplace=True)
            df.reset_index(drop=True, inplace=True)
        
        return df
    except Exception as e:
        logger.error(f"读取 Excel 文件失败: {str(e)}", exc_info=True)
        raise

def try_parse_datetime(series_or_col: pd.Series, strict_format: str = '%Y-%m-%d %H:%M:%S') -> pd.Series:
    """
    多级尝试解析日期时间
    """
    try:
        # 1. 首先尝试严格格式
        out = pd.to_datetime(series_or_col, format=strict_format, errors='coerce').dt.tz_localize(None)
        
        # 2. 检查解析失败率
        na_ratio = out.isna().mean()
        if na_ratio > 0.5:
            logger.warning(f"严格格式解析失败率较高({na_ratio:.0%})，尝试自动解析...")
            # 自动识别格式
            out = pd.to_datetime(series_or_col, errors='coerce').dt.tz_localize(None)
            
        return out
    except Exception as e:
        logger.warning(f"严格格式解析失败: {str(e)}，尝试自动解析...")
        return pd.to_datetime(series_or_col, errors='coerce').dt.tz_localize(None)

def parse_datetime_from_cols(df: pd.DataFrame, 
                           date_col: str = '订单提交日期',
                           time_col: str = '订单提交时间',
                           new_col: str = '提交时间') -> pd.DataFrame:
    """
    智能解析日期时间列
    """
    try:
        # 1. 数据预处理
        df[date_col] = df[date_col].astype(str).str.strip()
        df[time_col] = df[time_col].astype(str).str.strip()
        
        # 2. 检查时间列是否包含完整日期时间
        def looks_like_full_datetime(s: str) -> bool:
            return bool(re.match(r'^\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}:\d{1,2}$', s))
        
        sample_times = df[time_col].dropna().astype(str).head(50)
        full_dt_count = sum(looks_like_full_datetime(x) for x in sample_times)
        
        # 3. 根据情况选择解析策略
        if full_dt_count > len(sample_times) * 0.7:
            logger.info(f"检测到 {time_col} 列包含完整日期时间，直接解析该列")
            df[new_col] = try_parse_datetime(df[time_col])
        else:
            logger.info(f"合并 {date_col} 和 {time_col} 列进行解析")
            dt_str_col = df[date_col] + ' ' + df[time_col]
            df[new_col] = try_parse_datetime(dt_str_col)
        
        # 4. 处理无效记录
        na_count = df[new_col].isna().sum()
        if na_count > 0:
            invalid_examples = df[df[new_col].isna()][[date_col, time_col]].head()
            logger.warning(
                f"发现 {na_count} 行无效日期时间记录，示例:\n"
                f"{invalid_examples.to_string()}\n"
                f"总行数: {len(df)}, 有效行数: {len(df) - na_count}"
            )
            df = df.dropna(subset=[new_col]).copy()
        
        # 5. 验证时间范围
        if not df.empty:
            min_time = df[new_col].min()
            max_time = df[new_col].max()
            logger.info(f"解析后的时间范围: {min_time} 至 {max_time}")
        
        return df
    except Exception as e:
        logger.error(f"解析日期时间失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

async def process_data_in_background(task_id: str, order_file_path: str, schedule_file_path: str):
    """后台处理数据的函数"""
    try:
        # 更新任务状态为处理中
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 0,
            'message': '开始处理文件...',
            'start_time': datetime.now().isoformat()
        })

        # 1. 读取并验证文件
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 5,
            'message': '读取文件...'
        })
        
        # 读取订单文件
        order_df = read_excel_with_header_fix(order_file_path)
        required_order_columns = ['主订单编号', '子订单编号', '商品ID', '选购商品', 
                                '流量来源', '流量体裁', '取消原因', '订单状态', 
                                '订单应付金额', '订单提交日期', '订单提交时间']
        
        if not all(col in order_df.columns for col in required_order_columns):
            raise ValueError('订单文件缺少必需的列')
            
        # 读取排班表
        schedule_df = read_excel_with_header_fix(schedule_file_path)
        required_schedule_columns = ['日期', '上播时间', '下播时间', 
                                   '主播姓名', '场控姓名', '时段消耗']
        
        if not all(col in schedule_df.columns for col in required_schedule_columns):
            raise ValueError('排班表缺少必需的列')

        # 2. 处理排班数据
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 20,
            'message': '处理排班数据...'
        })
        
        # 解析上播时间
        schedule_df = parse_datetime_from_cols(
            schedule_df,
            date_col='日期',
            time_col='上播时间',
            new_col='上播时间'
        )
        
        # 解析下播时间
        schedule_df = parse_datetime_from_cols(
            schedule_df,
            date_col='日期',
            time_col='下播时间',
            new_col='下播时间'
        )
        
        # 处理跨天直播
        cross_day = schedule_df['下播时间'] < schedule_df['上播时间']
        if cross_day.any():
            schedule_df.loc[cross_day, '下播时间'] += pd.Timedelta(days=1)
            logger.info(f"发现 {cross_day.sum()} 条跨天直播记录")
        
        # 3. 分块处理订单数据
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 40,
            'message': '处理订单数据...'
        })
        
        chunk_size = 1000
        processed_chunks = []
        total_chunks = len(order_df) // chunk_size + 1
        
        for chunk_idx in range(total_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min((chunk_idx + 1) * chunk_size, len(order_df))
            
            chunk_df = order_df.iloc[start_idx:end_idx].copy()
            
            # 解析订单时间
            chunk_df = parse_datetime_from_cols(
                chunk_df,
                date_col='订单提交日期',
                time_col='订单提交时间',
                new_col='提交时间'
            )
            
            # 处理订单数据
            processed_chunk = process_chunk(chunk_df, schedule_df)
            if not processed_chunk.empty:
                processed_chunks.append(processed_chunk)
            
            # 更新进度
            progress = 40 + int(chunk_idx * 40 / total_chunks)
            await set_task_status(task_id, {
                'status': 'processing',
                'progress': progress,
                'message': f'已处理 {chunk_idx + 1}/{total_chunks} 个数据块...'
            })
            
            # 清理内存
            gc.collect()
            await asyncio.sleep(0.1)
        
        # 4. 合并结果
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 80,
            'message': '合并处理结果...'
        })
        
        final_df = pd.concat(processed_chunks, ignore_index=True) if processed_chunks else pd.DataFrame()
        
        # 5. 生成结果文件
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 90,
            'message': '生成结果文件...'
        })
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            final_df.to_excel(tmp_file.name, index=False)
            
            with open(tmp_file.name, 'rb') as f:
                result_data = f.read()
            
            os.unlink(tmp_file.name)
        
        # 6. 完成处理
        await set_task_status(task_id, {
            'status': 'completed',
            'progress': 100,
            'message': '处理完成',
            'completion_time': datetime.now().isoformat(),
            'result': base64.b64encode(result_data).decode('utf-8')
        })
        
    except Exception as e:
        logger.error(f"处理失败: {str(e)}", exc_info=True)
        await set_task_status(task_id, {
            'status': 'failed',
            'progress': 0,
            'message': f'处理失败: {str(e)}',
            'failure_time': datetime.now().isoformat()
        })
        try:
            os.unlink(order_file_path)
            os.unlink(schedule_file_path)
        except:
            pass

def process_chunk(chunk_df: pd.DataFrame, schedule_df: pd.DataFrame) -> pd.DataFrame:
    """处理单个数据块"""
    try:
        if chunk_df.empty:
            return pd.DataFrame()
        
        # 匹配订单与排班
        results = []
        total_orders = len(chunk_df)
        matched_orders = 0
        
        for _, order in chunk_df.iterrows():
            # 查找对应的排班记录
            matched = schedule_df[
                (schedule_df['上播时间'] <= order['提交时间']) & 
                (schedule_df['下播时间'] >= order['提交时间'])
            ]
            
            if not matched.empty:
                matched_orders += 1
                schedule = matched.iloc[0]
                result_row = {
                    '订单编号': order['主订单编号'],
                    '子订单编号': order['子订单编号'],
                    '商品ID': order['商品ID'],
                    '商品名称': order['选购商品'],
                    '订单金额': order['订单应付金额'],
                    '订单状态': order['订单状态'],
                    '提交时间': order['提交时间'].strftime('%Y-%m-%d %H:%M:%S'),
                    '主播': schedule['主播姓名'],
                    '场控': schedule['场控姓名'],
                    '直播时段': f"{schedule['上播时间'].strftime('%Y-%m-%d %H:%M:%S')} - {schedule['下播时间'].strftime('%Y-%m-%d %H:%M:%S')}",
                    '时段消耗': schedule['时段消耗']
                }
                results.append(result_row)
            else:
                # 记录未匹配订单的信息
                logger.warning(
                    f"订单时间 {order['提交时间']} 未找到匹配的排班记录\n"
                    f"订单信息: {order.to_string()}\n"
                    f"最近的排班记录:\n"
                    f"上一个时段: {schedule_df[schedule_df['下播时间'] < order['提交时间']].iloc[-1:].to_string() if not schedule_df[schedule_df['下播时间'] < order['提交时间']].empty else '无'}\n"
                    f"下一个时段: {schedule_df[schedule_df['上播时间'] > order['提交时间']].iloc[:1].to_string() if not schedule_df[schedule_df['上播时间'] > order['提交时间']].empty else '无'}"
                )
        
        # 记录匹配率
        match_rate = matched_orders / total_orders if total_orders > 0 else 0
        logger.info(f"订单匹配率: {match_rate:.2%} ({matched_orders}/{total_orders})")
        
        return pd.DataFrame(results)
        
    except Exception as e:
        logger.error(f"处理数据块时出错: {str(e)}", exc_info=True)
        return pd.DataFrame()

# 配置内存日志处理器
class MemoryLogHandler(logging.Handler):
    def emit(self, record):
        global log_id_counter
        try:
            log_id_counter += 1
            log_entry = {
                "id": log_id_counter,
                "level": record.levelname.lower(),
                "message": self.format(record),
                "timestamp": time.time()
            }
            log_buffer.append(log_entry)
            # 保持最多1000条日志
            if len(log_buffer) > 1000:
                log_buffer.pop(0)
        except Exception as e:
            print(f"内存日志处理失败: {str(e)}")

# 添加内存日志处理器
memory_handler = MemoryLogHandler()
logger.addHandler(memory_handler)
