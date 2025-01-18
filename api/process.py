import os
import uuid
import time
import json
import logging
import traceback
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, File, UploadFile, BackgroundTasks, Request, WebSocket, HTTPException
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
import datetime
import gc

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
        # 1. 首先通过 Content-Length 快速判断文件大小
        content_length = request.headers.get('content-length')
        if content_length and int(content_length) > 200 * 1024 * 1024:  # 200MB (两个文件总和)
            raise HTTPException(status_code=413, detail="上传文件总大小超过限制")
            
        # 2. 验证文件扩展名
        if not order_file.filename.endswith('.xlsx'):
            raise HTTPException(status_code=400, detail="订单文件必须是 Excel 文件(.xlsx)")
        if not schedule_file.filename.endswith('.xlsx'):
            raise HTTPException(status_code=400, detail="排班表必须是 Excel 文件(.xlsx)")
            
        # 3. 生成任务ID
        task_id = str(uuid.uuid4())
        logger.info(f"开始处理任务 {task_id}")
        
        # 4. 保存文件到临时目录
        order_temp = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        schedule_temp = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        
        try:
            # 5. 分块读取并写入文件
            max_size = 100 * 1024 * 1024  # 单个文件最大 100MB
            chunk_size = 1024 * 1024  # 每次读取 1MB
            
            # 处理订单文件
            uploaded_size = 0
            while True:
                chunk = await order_file.read(chunk_size)
                if not chunk:
                    break
                uploaded_size += len(chunk)
                if uploaded_size > max_size:
                    raise HTTPException(status_code=413, detail="订单文件大小超过限制")
                order_temp.write(chunk)
                
            # 处理排班表文件
            uploaded_size = 0
            while True:
                chunk = await schedule_file.read(chunk_size)
                if not chunk:
                    break
                uploaded_size += len(chunk)
                if uploaded_size > max_size:
                    raise HTTPException(status_code=413, detail="排班表文件大小超过限制")
                schedule_temp.write(chunk)
                
            # 关闭文件
            order_temp.close()
            schedule_temp.close()
            
            logger.info(f"文件上传完成: order_file={order_file.filename}, schedule_file={schedule_file.filename}")
            
            # 6. 初始化任务状态
            await set_task_status(task_id, {
                'status': 'processing',
                'progress': 0,
                'message': '开始处理...',
                'start_time': datetime.now().isoformat()
            })
            
            # 7. 启动后台处理
            asyncio.create_task(process_data_in_background(
                task_id=task_id,
                order_file_path=order_temp.name,
                schedule_file_path=schedule_temp.name
            ))
            
            # 8. 立即返回任务ID
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
        status = await get_task_status(task_id)
        if not status:
            return JSONResponse(
                status_code=404,
                content={"error": "任务不存在"}
            )
        
        # 根据任务状态返回不同的响应
        if status["status"] == "completed":
            result = status.get("result")
            if result:
                return JSONResponse(content={
                    "status": "completed",
                    "progress": 100,
                    "message": "处理完成",
                    "result": result,
                    "filename": "processed_result.xlsx"
                })
            return JSONResponse(content={
                "status": "completed",
                "progress": 100,
                "message": "处理完成"
            })
        elif status["status"] == "failed":
            return JSONResponse(
                status_code=500,
                content={
                    "status": "failed",
                    "message": status.get("message", "处理失败"),
                    "progress": 0
                }
            )
        else:
            return JSONResponse(content={
                "status": status["status"],
                "progress": status.get("progress", 0),
                "message": status.get("message", "正在处理...")
            })
    except Exception as e:
        logger.error(f"获取任务状态失败: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"获取任务状态失败：{str(e)}",
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

        # 1. 首先验证文件格式
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 5,
            'message': '验证文件格式...'
        })
        
        # 读取订单文件头部进行验证
        order_df = pd.read_excel(order_file_path, nrows=1)
        required_order_columns = ['主订单编号', '子订单编号', '商品ID', '选购商品', 
                                '流量来源', '流量体裁', '取消原因', '订单状态', 
                                '订单应付金额', '订单提交日期', '订单提交时间']
        
        if not all(col in order_df.columns for col in required_order_columns):
            raise ValueError('订单文件缺少必需的列')
            
        # 读取排班表头部进行验证    
        schedule_df = pd.read_excel(schedule_file_path, nrows=1)
        required_schedule_columns = ['日期', '上播时间', '下播时间', 
                                   '主播姓名', '场控姓名', '时段消耗']
        
        if not all(col in schedule_df.columns for col in required_schedule_columns):
            raise ValueError('排班表缺少必需的列')

        # 2. 分块读取订单数据
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 10,
            'message': '开始读取订单数据...'
        })
        
        chunk_size = 1000  # 每次处理1000行
        order_chunks = []
        
        for chunk_idx, chunk in enumerate(pd.read_excel(order_file_path, chunksize=chunk_size)):
            order_chunks.append(chunk)
            progress = 10 + int((chunk_idx + 1) * 20 / len(order_chunks))  # 读取订单占20%进度
            await set_task_status(task_id, {
                'status': 'processing',
                'progress': progress,
                'message': f'已读取 {(chunk_idx + 1) * chunk_size} 条订单数据...'
            })
            # 释放内存
            gc.collect()
            await asyncio.sleep(0.1)  # 让出控制权
            
        # 3. 读取排班数据
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 30,
            'message': '读取排班数据...'
        })
        
        schedule_df = pd.read_excel(schedule_file_path)
        
        # 4. 数据处理和匹配
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 40,
            'message': '开始处理数据...'
        })
        
        processed_chunks = []
        total_chunks = len(order_chunks)
        
        for idx, chunk in enumerate(order_chunks):
            # 处理每个分块
            processed_chunk = process_chunk(chunk, schedule_df)
            processed_chunks.append(processed_chunk)
            
            progress = 40 + int((idx + 1) * 40 / total_chunks)  # 处理过程占40%进度
            await set_task_status(task_id, {
                'status': 'processing',
                'progress': progress,
                'message': f'正在处理第 {idx + 1}/{total_chunks} 块数据...'
            })
            
            # 释放内存
            del chunk
            gc.collect()
            await asyncio.sleep(0.1)  # 让出控制权
            
        # 5. 合并结果
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 80,
            'message': '合并处理结果...'
        })
        
        final_df = pd.concat(processed_chunks, ignore_index=True)
        
        # 6. 生成结果文件
        await set_task_status(task_id, {
            'status': 'processing',
            'progress': 90,
            'message': '生成结果文件...'
        })
        
        # 创建一个临时文件来保存结果
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            final_df.to_excel(tmp_file.name, index=False)
            
            # 读取生成的文件
            with open(tmp_file.name, 'rb') as f:
                result_data = f.read()
                
            # 删除临时文件
            os.unlink(tmp_file.name)
        
        # 7. 更新任务状态为完成
        await set_task_status(task_id, {
            'status': 'completed',
            'progress': 100,
            'message': '处理完成',
            'completion_time': datetime.now().isoformat(),
            'result': base64.b64encode(result_data).decode('utf-8')
        })
        
    except Exception as e:
        logger.error(f"处理失败: {str(e)}", exc_info=True)
        # 更新任务状态为失败
        await set_task_status(task_id, {
            'status': 'failed',
            'progress': 0,
            'message': f'处理失败: {str(e)}',
            'failure_time': datetime.now().isoformat()
        })
        # 清理临时文件
        try:
            os.unlink(order_file_path)
            os.unlink(schedule_file_path)
        except:
            pass

def process_chunk(chunk_df: pd.DataFrame, schedule_df: pd.DataFrame) -> pd.DataFrame:
    """处理单个数据块的函数"""
    try:
        # 1. 数据清洗
        chunk_df['订单提交时间'] = pd.to_datetime(
            chunk_df['订单提交日期'].astype(str) + ' ' + 
            chunk_df['订单提交时间'].astype(str)
        )
        
        # 2. 匹配订单与排班
        results = []
        for _, order in chunk_df.iterrows():
            # 查找对应的排班记录
            matched_schedule = find_matching_schedule(order, schedule_df)
            if matched_schedule is not None:
                result_row = {
                    '订单编号': order['主订单编号'],
                    '子订单编号': order['子订单编号'],
                    '商品ID': order['商品ID'],
                    '商品名称': order['选购商品'],
                    '订单金额': order['订单应付金额'],
                    '订单状态': order['订单状态'],
                    '提交时间': order['订单提交时间'],
                    '主播': matched_schedule['主播姓名'],
                    '场控': matched_schedule['场控姓名'],
                    '直播时段': f"{matched_schedule['上播时间']} - {matched_schedule['下播时间']}",
                    '时段消耗': matched_schedule['时段消耗']
                }
                results.append(result_row)
        
        return pd.DataFrame(results)
        
    except Exception as e:
        logger.error(f"处理数据块时出错: {str(e)}", exc_info=True)
        raise

def find_matching_schedule(order: pd.Series, schedule_df: pd.DataFrame) -> Optional[pd.Series]:
    """查找订单对应的排班记录"""
    try:
        order_time = order['订单提交时间']
        
        # 转换排班表的时间
        schedule_df['上播时间'] = pd.to_datetime(
            schedule_df['日期'].astype(str) + ' ' + 
            schedule_df['上播时间'].astype(str)
        )
        schedule_df['下播时间'] = pd.to_datetime(
            schedule_df['日期'].astype(str) + ' ' + 
            schedule_df['下播时间'].astype(str)
        )
        
        # 查找订单时间在直播时段内的记录
        matched = schedule_df[
            (schedule_df['上播时间'] <= order_time) & 
            (schedule_df['下播时间'] >= order_time)
        ]
        
        if not matched.empty:
            return matched.iloc[0]
        return None
        
    except Exception as e:
        logger.error(f"匹配排班记录时出错: {str(e)}", exc_info=True)
        return None

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
