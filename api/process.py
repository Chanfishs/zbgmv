import os
import uuid
import time
import json
import logging
import traceback
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, File, UploadFile, BackgroundTasks, Request, WebSocket
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
async def handle_upload(background_tasks: BackgroundTasks, order_file: UploadFile = File(...), schedule_file: UploadFile = File(...)):
    """处理文件上传"""
    try:
        logger.info(f"开始处理文件上传: order_file={order_file.filename}, schedule_file={schedule_file.filename}")
        
        # 验证文件扩展名
        if not order_file.filename.endswith('.xlsx') or not schedule_file.filename.endswith('.xlsx'):
            logger.error("文件格式错误：非 .xlsx 格式")
            return JSONResponse(
                status_code=400,
                content={"error": "文件格式错误：请上传 .xlsx 格式的文件"}
            )

        # 读取文件内容
        try:
            order_data = await order_file.read()
            schedule_data = await schedule_file.read()
        except Exception as e:
            logger.error(f"文件读取失败: {str(e)}")
            return JSONResponse(
                status_code=400,
                content={"error": f"文件读取失败：{str(e)}"}
            )

        # 验证文件大小
        MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
        if len(order_data) > MAX_FILE_SIZE or len(schedule_data) > MAX_FILE_SIZE:
            logger.error("文件过大")
            return JSONResponse(
                status_code=400,
                content={"error": "文件过大：请确保每个文件小于100MB"}
            )

        try:
            # 创建任务ID并初始化状态
            task_id = str(uuid.uuid4())
            initial_status = {
                "status": TASK_STATUS_PENDING,
                "progress": 0,
                "message": "正在准备处理...",
                "start_time": time.time()
            }
            
            # 设置任务状态
            await set_task_status(task_id, initial_status)
            
            # 启动后台任务
            background_tasks.add_task(process_data_in_background, task_id, order_data, schedule_data)
            
            return JSONResponse(content={
                "task_id": task_id,
                "message": "文件已接收，正在处理中"
            })
        except Exception as e:
            logger.error(f"任务创建失败: {str(e)}")
            return JSONResponse(
                status_code=500,
                content={"error": f"任务创建失败：{str(e)}"}
            )
    except Exception as e:
        logger.error(f"系统错误: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"系统错误：{str(e)}"}
        )

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

async def process_data_in_background(task_id: str, order_data: bytes, schedule_data: bytes):
    """后台处理数据"""
    try:
        # 更新任务状态为处理中
        await set_task_status(task_id, {
            "status": TASK_STATUS_PROCESSING,
            "progress": 10,
            "message": "正在读取文件数据..."
        })

        # 读取 Excel 文件数据
        try:
            order_df = pd.read_excel(io.BytesIO(order_data))
            schedule_df = pd.read_excel(io.BytesIO(schedule_data))
            
            await set_task_status(task_id, {
                "status": TASK_STATUS_PROCESSING,
                "progress": 30,
                "message": "文件读取完成，正在处理数据..."
            })
        except Exception as e:
            logger.error(f"Excel 文件读取失败: {str(e)}")
            await set_task_status(task_id, {
                "status": TASK_STATUS_FAILED,
                "message": f"Excel 文件读取失败：{str(e)}",
                "end_time": time.time()
            })
            return

        # 数据处理逻辑
        try:
            # TODO: 在这里添加具体的数据处理逻辑
            # 示例：简单的数据合并
            result_df = pd.DataFrame({
                "处理时间": pd.Timestamp.now(),
                "订单数量": len(order_df),
                "排班数量": len(schedule_df)
            }, index=[0])
            
            # 将结果保存为 Excel
            output = io.BytesIO()
            result_df.to_excel(output, index=False)
            result_base64 = base64.b64encode(output.getvalue()).decode()
            
            # 更新任务状态为完成
            await set_task_status(task_id, {
                "status": TASK_STATUS_COMPLETED,
                "progress": 100,
                "message": "处理完成",
                "result": result_base64,
                "end_time": time.time()
            })
            
        except Exception as e:
            logger.error(f"数据处理失败: {str(e)}")
            await set_task_status(task_id, {
                "status": TASK_STATUS_FAILED,
                "message": f"数据处理失败：{str(e)}",
                "end_time": time.time()
            })
            return
            
    except Exception as e:
        logger.error(f"后台处理失败: {str(e)}")
        try:
            await set_task_status(task_id, {
                "status": TASK_STATUS_FAILED,
                "message": f"后台处理失败：{str(e)}",
                "end_time": time.time()
            })
        except:
            pass

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
