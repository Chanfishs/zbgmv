from contextlib import asynccontextmanager
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, Request, HTTPException, WebSocket
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
import traceback
import sys
import logging
from typing import Dict, Set, Optional, List

# 配置日志格式
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

# 尝试导入 psutil，如果不可用则跳过
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    print("[WARNING] psutil 模块不可用，系统资源监控功能将被禁用")

# Redis 连接配置和重试机制
MAX_RETRIES = 3
RETRY_DELAY = 1  # 秒

# Redis 连接配置
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

# 必需的列名定义
required_order_columns = [
    "主订单编号",
    "子订单编号",
    "商品ID",
    "选购商品",
    "流量来源",
    "流量体裁",
    "取消原因",
    "订单状态",
    "订单应付金额",
    "订单提交日期",
    "订单提交时间",
]
required_schedule_columns = ["日期", "上播时间", "下播时间", "主播姓名", "场控姓名", "时段消耗"]

# 任务状态常量
TASK_STATUS_PENDING = "pending"
TASK_STATUS_PROCESSING = "processing"
TASK_STATUS_COMPLETED = "completed"
TASK_STATUS_FAILED = "failed"
TASK_STATUS_CANCELLED = "cancelled"

# 任务过期时间（秒）
TASK_EXPIRY = 3600  # 1小时

# 存储正在运行的任务
running_tasks = {}

print("[DEBUG] ===== Redis 配置信息 =====")
print(f"[DEBUG] UPSTASH_REDIS_REST_URL: {UPSTASH_REDIS_REST_URL}")
print(
    f"[DEBUG] UPSTASH_REDIS_REST_TOKEN: {'***' + UPSTASH_REDIS_REST_TOKEN[-8:] if UPSTASH_REDIS_REST_TOKEN else 'None'}"
)

# 创建 Redis 客户端
redis = None

def get_redis_client():
    """获取 Redis 客户端实例，带重试机制"""
    global redis
    retry_count = 0
    last_error = None
    
    while retry_count < MAX_RETRIES:
        try:
            if not redis:
                if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
                    logger.error("Redis 配置未设置，请检查环境变量")
                    raise Exception("请检查 UPSTASH_REDIS_REST_URL 和 UPSTASH_REDIS_REST_TOKEN 环境变量")

                logger.info(f"正在初始化 Redis 客户端 (尝试 {retry_count + 1}/{MAX_RETRIES})")
                logger.debug(f"Redis URL: {UPSTASH_REDIS_REST_URL}")
                logger.debug(f"Redis Token: ***{UPSTASH_REDIS_REST_TOKEN[-8:]}")
                
                redis = Redis(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)

                # 测试连接
                test_key = f"test:init:{uuid.uuid4()}"
                test_value = f"connection_test_{time.time()}"
                logger.debug(f"测试 Redis 连接，key: {test_key}")
                
                redis.set(test_key, test_value, ex=60)  # 60秒后过期
                result = redis.get(test_key)
                redis.delete(test_key)

                if result != test_value:
                    raise Exception(f"Redis 连接测试失败: 期望值 '{test_value}'，实际值 '{result}'")

                logger.info("Redis 客户端初始化成功")
                return redis
            return redis
            
        except Exception as e:
            last_error = str(e)
            logger.error(f"Redis 客户端初始化失败 (尝试 {retry_count + 1}/{MAX_RETRIES}): {last_error}")
            retry_count += 1
            if retry_count < MAX_RETRIES:
                logger.info(f"等待 {RETRY_DELAY} 秒后重试...")
                time.sleep(RETRY_DELAY)
            redis = None

    logger.error(f"Redis 客户端初始化失败，已达到最大重试次数: {last_error}")
    raise Exception(f"Redis 连接失败: {last_error}")

async def get_task_status(task_id: str) -> dict:
    """从Redis获取任务状态，带重试机制"""
    retry_count = 0
    last_error = None
    
    while retry_count < MAX_RETRIES:
        try:
            redis_client = get_redis_client()
            key = f"task_status:{task_id}"
            logger.debug(f"获取任务状态，key: {key}")

            status = redis_client.get(key)
            if not status:
                logger.debug(f"未找到任务状态: {key}")
                return {"status": "not_found"}

            try:
                status_dict = json.loads(status)
                logger.debug(f"成功解析任务状态: {status_dict}")
            except json.JSONDecodeError as e:
                logger.error(f"解析任务状态失败: {str(e)}")
                return {"status": "error", "message": "任务状态格式错误"}

            # 如果存在大型结果数据，则重新组装
            if status_dict.get("has_large_result"):
                info_key = f"task_result:{task_id}:info"
                logger.debug(f"获取分块信息，key: {info_key}")
                result_info = redis_client.get(info_key)

                if result_info:
                    info = json.loads(result_info)
                    total_chunks = info["total_chunks"]
                    logger.info(f"开始重组 {total_chunks} 个数据块")

                    result_data = ""
                    for i in range(total_chunks):
                        chunk_key = f"task_result:{task_id}:chunk:{i}"
                        chunk = redis_client.get(chunk_key)
                        if chunk:
                            result_data += chunk
                        else:
                            logger.warning(f"未找到数据块 {i}")

                    status_dict["result"] = result_data
                    del status_dict["has_large_result"]
                    logger.debug(f"数据重组完成，总大小: {len(result_data)} bytes")

            return status_dict
            
        except Exception as e:
            last_error = str(e)
            logger.error(f"获取任务状态失败 (尝试 {retry_count + 1}/{MAX_RETRIES}): {last_error}")
            retry_count += 1
            if retry_count < MAX_RETRIES:
                logger.info(f"等待 {RETRY_DELAY} 秒后重试...")
                await asyncio.sleep(RETRY_DELAY)

    logger.error(f"获取任务状态失败，已达到最大重试次数: {last_error}")
    return {"status": "error", "message": f"获取任务状态失败: {last_error}"}

async def set_task_status(task_id: str, status: dict):
    """设置任务状态到Redis，带重试机制"""
    retry_count = 0
    last_error = None
    
    while retry_count < MAX_RETRIES:
        try:
            redis_client = get_redis_client()
            key = f"task_status:{task_id}"
            logger.debug(f"设置任务状态，key: {key}, status: {status}")

            # 如果状态包含结果数据且大小超过 900KB，则分块存储
            if "result" in status and len(str(status["result"])) > 900000:
                logger.info(f"检测到大型结果数据 ({len(str(status['result']))} bytes)，开始分块存储")

                # 存储主状态信息，不包含结果数据
                main_status = {k: v for k, v in status.items() if k != "result"}
                main_status["has_large_result"] = True
                main_status["update_time"] = time.time()

                redis_client.setex(
                    key,
                    TASK_EXPIRY,
                    json.dumps(main_status),
                )
                logger.debug("已存储主状态信息")

                # 分块存储结果数据
                result_data = status["result"]
                chunk_size = 800000  # 每块约800KB
                chunks = [result_data[i:i + chunk_size] for i in range(0, len(result_data), chunk_size)]

                logger.info(f"开始存储 {len(chunks)} 个数据块")
                for i, chunk in enumerate(chunks):
                    chunk_key = f"task_result:{task_id}:chunk:{i}"
                    redis_client.setex(
                        chunk_key,
                        TASK_EXPIRY,
                        chunk,
                    )
                    logger.debug(f"已存储数据块 {i+1}/{len(chunks)}")

                # 存储分块信息
                info_key = f"task_result:{task_id}:info"
                redis_client.setex(
                    info_key,
                    TASK_EXPIRY,
                    json.dumps({"total_chunks": len(chunks), "update_time": time.time()}),
                )
                logger.debug("已存储分块信息")
            else:
                # 正常存储小型状态数据
                status["update_time"] = time.time()
                redis_client.setex(
                    key,
                    TASK_EXPIRY,
                    json.dumps(status),
                )
                logger.debug(f"已存储状态数据，大小: {len(str(status))} bytes")
            
            return
            
        except Exception as e:
            last_error = str(e)
            logger.error(f"设置任务状态失败 (尝试 {retry_count + 1}/{MAX_RETRIES}): {last_error}")
            retry_count += 1
            if retry_count < MAX_RETRIES:
                logger.info(f"等待 {RETRY_DELAY} 秒后重试...")
                await asyncio.sleep(RETRY_DELAY)

    logger.error(f"设置任务状态失败，已达到最大重试次数: {last_error}")
    raise Exception(f"设置任务状态失败: {last_error}")

# 创建 FastAPI 应用实例
app = FastAPI()

# 配置静态文件
app.mount("/static", StaticFiles(directory="api/static"), name="static")

# 配置模板
templates = Jinja2Templates(directory="api/templates")

# WebSocket 连接管理
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.message_queue: List[Dict] = []
        self.max_queue_size = 1000

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket client connected. Total connections: {len(self.active_connections)}")
        
        # 发送队列中的历史消息
        for message in self.message_queue:
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"Error sending queued message: {str(e)}")
                break

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket client disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: str, level: str = "info"):
        data = {
            "message": message,
            "level": level,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 添加到消息队列
        self.message_queue.append(data)
        if len(self.message_queue) > self.max_queue_size:
            self.message_queue.pop(0)
        
        # 广播给所有连接
        for connection in list(self.active_connections):
            try:
                await connection.send_json(data)
            except Exception as e:
                logger.error(f"Error broadcasting message: {str(e)}")
                self.active_connections.remove(connection)

# 创建连接管理器实例
manager = ConnectionManager()

# 自定义日志处理器
class WebSocketLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            # 使用 asyncio.create_task 在事件循环中运行广播
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(
                    manager.broadcast(msg, record.levelname.lower())
                )
            else:
                asyncio.run(
                    manager.broadcast(msg, record.levelname.lower())
                )
        except Exception as e:
            print(f"Error in WebSocketLogHandler: {str(e)}")

# 添加WebSocket日志处理器
ws_handler = WebSocketLogHandler()
ws_handler.setFormatter(
    logging.Formatter("%(message)s")
)
logger.addHandler(ws_handler)

# 日志存储
log_buffer = []
log_id_counter = 0

def add_log(message: str, level: str = "info"):
    global log_id_counter
    log_id_counter += 1
    log_entry = {
        "id": log_id_counter,
        "message": message,
        "level": level,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    log_buffer.append(log_entry)
    # 保持最近1000条日志
    if len(log_buffer) > 1000:
        log_buffer.pop(0)
    return log_entry

# 自定义日志处理器
class BufferLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            add_log(msg, record.levelname.lower())
        except Exception as e:
            print(f"Error in BufferLogHandler: {str(e)}")

# 添加缓冲日志处理器
buffer_handler = BufferLogHandler()
buffer_handler.setFormatter(
    logging.Formatter("%(message)s")
)
logger.addHandler(buffer_handler)

# 日志获取接口
@app.get("/api/logs")
async def get_logs(since: int = 0):
    """获取指定ID之后的日志"""
    return [log for log in log_buffer if log["id"] > since]

# 测试日志输出
@app.on_event("startup")
async def startup_event():
    """应用程序启动时的事件处理"""
    try:
        logger.info("应用程序启动")
        logger.info("正在初始化 Redis 连接...")
        global redis
        redis = get_redis_client()
        logger.info("Redis 连接初始化完成")
    except Exception as e:
        logger.error(f"应用程序启动失败: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """应用程序关闭时的事件处理"""
    global redis
    if redis:
        print("[DEBUG] 正在关闭 Redis 连接")
        # Redis 客户端不需要异步关闭
        redis = None
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

async def monitor_system_resources():
    """监控系统资源使用情况"""
    if not HAS_PSUTIL:
        return {
            "status": "disabled",
            "message": "系统资源监控功能未启用（psutil 模块不可用）"
        }
    
    try:
        process = psutil.Process(os.getpid())
        return {
            "status": "success",
            "cpu_percent": psutil.cpu_percent(),
            "memory_usage_mb": process.memory_info().rss / 1024 / 1024,
            "memory_percent": psutil.virtual_memory().percent,
            "disk_usage_percent": psutil.disk_usage("/").percent
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"监控系统资源失败: {str(e)}"
        }

@app.post("/api/process")
async def handle_upload(background_tasks: BackgroundTasks, order_file: UploadFile = File(...), schedule_file: UploadFile = File(...)):
    """处理文件上传"""
    try:
        # 检查文件是否为空
        if not order_file or not schedule_file:
            logger.error("文件上传失败：未收到文件")
            return JSONResponse(
                status_code=400,
                content={"error": "请选择要上传的文件"}
            )

        # 检查文件名是否为空
        if not order_file.filename or not schedule_file.filename:
            logger.error(f"文件名为空：order_file={order_file.filename}, schedule_file={schedule_file.filename}")
            return JSONResponse(
                status_code=400,
                content={"error": "文件名不能为空"}
            )

        logger.info(f"开始处理文件上传: order_file={order_file.filename}, schedule_file={schedule_file.filename}")
        logger.debug(f"文件类型: order_content_type={order_file.content_type}, schedule_content_type={schedule_file.content_type}")
        logger.debug(f"环境变量检查: REDIS_URL={bool(UPSTASH_REDIS_REST_URL)}, REDIS_TOKEN={bool(UPSTASH_REDIS_REST_TOKEN)}")
        
        # 获取 Redis 客户端
        redis_client = get_redis_client()
        if not redis_client:
            logger.error("Redis 客户端初始化失败")
            raise Exception("Redis 客户端初始化失败")
        
        logger.info("Redis 连接成功")
        
        # 验证文件扩展名（不区分大小写）
        if not (order_file.filename.lower().endswith('.xlsx') and schedule_file.filename.lower().endswith('.xlsx')):
            logger.error(f"文件格式错误：order_file={order_file.filename}, schedule_file={schedule_file.filename}")
            return JSONResponse(
                status_code=400,
                content={"error": "文件格式错误：请上传 .xlsx 格式的文件"}
            )

        # 读取文件内容
        try:
            logger.debug("开始读取订单文件...")
            order_data = await order_file.read()
            logger.info(f"订单文件读取完成，大小: {len(order_data)} bytes")
            
            logger.debug("开始读取排班表文件...")
            schedule_data = await schedule_file.read()
            logger.info(f"排班表文件读取完成，大小: {len(schedule_data)} bytes")

            # 检查文件是否为空
            if len(order_data) == 0 or len(schedule_data) == 0:
                logger.error("文件内容为空")
                return JSONResponse(
                    status_code=400,
                    content={"error": "上传的文件内容为空"}
                )

        except Exception as e:
            logger.error(f"文件读取失败: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return JSONResponse(
                status_code=400,
                content={"error": f"文件读取失败：{str(e)}"}
            )

        # 验证文件大小
        MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
        if len(order_data) > MAX_FILE_SIZE or len(schedule_data) > MAX_FILE_SIZE:
            logger.error(f"文件过大: order_size={len(order_data)}, schedule_size={len(schedule_data)}")
            return JSONResponse(
                status_code=400,
                content={"error": "文件过大：请确保每个文件小于100MB"}
            )

        try:
            # 创建任务ID并初始化状态
            task_id = str(uuid.uuid4())
            logger.info(f"创建新任务: task_id={task_id}")
            
            # 初始化任务状态
            initial_status = {
                "status": TASK_STATUS_PENDING,
                "progress": 0,
                "message": "正在准备处理...",
                "start_time": time.time()
            }
            
            # 使用 redis_client 设置任务状态
            await set_task_status(task_id, initial_status)
            logger.info(f"任务状态初始化完成: {initial_status}")

            # 启动后台任务
            background_tasks.add_task(process_data_in_background, task_id, order_data, schedule_data)
            logger.info("后台任务已启动")

            # 返回任务ID
            return JSONResponse(
                content={
                    "task_id": task_id,
                    "message": "文件已接收，正在处理中"
                }
            )

        except Exception as e:
            logger.error(f"任务创建失败: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return JSONResponse(
                status_code=500,
                content={"error": f"任务创建失败：{str(e)}"}
            )

    except Exception as e:
        logger.error(f"系统错误: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
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

async def cancel_task(task_id: str):
    """取消正在运行的任务"""
    if task_id in running_tasks:
        task = running_tasks[task_id]
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                print(f"[DEBUG] 任务 {task_id} 已取消")
            finally:
                del running_tasks[task_id]
                await set_task_status(task_id, {
                    "status": TASK_STATUS_CANCELLED,
                    "message": "任务已取消",
                    "progress": 0
                })
                return True
    return False

@app.post("/api/cancel/{task_id}")
async def cancel_task_endpoint(task_id: str):
    """终止任务的API端点"""
    try:
        if await cancel_task(task_id):
            return JSONResponse(content={"message": "任务已终止"})
        else:
            return JSONResponse(
                status_code=404,
                content={"error": "任务不存在或已完成"}
            )
    except Exception as e:
        print(f"[ERROR] 终止任务失败: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"终止任务失败: {str(e)}"}
        )

async def process_data_in_background(task_id: str, order_data: bytes, schedule_data: bytes):
    """后台处理数据的异步函数"""
    try:
        # 将任务添加到运行中的任务字典
        task = asyncio.current_task()
        running_tasks[task_id] = task
        
        logger.info(f"开始处理任务 {task_id}")
        logger.info(f"订单数据大小: {len(order_data)} bytes")
        logger.info(f"排班表数据大小: {len(schedule_data)} bytes")
        
        # 更新任务状态为开始处理
        await set_task_status(task_id, {
            "status": TASK_STATUS_PROCESSING,
            "progress": 5,
            "message": "开始处理数据..."
        })
        
        try:
            # 读取订单数据
            logger.info("正在读取订单数据...")
            order_buffer = BytesIO(order_data)
            df_order = pd.read_excel(order_buffer, engine="openpyxl")
            logger.info(f"订单数据读取成功，共 {len(df_order)} 行")
            
            await set_task_status(task_id, {
                "status": TASK_STATUS_PROCESSING,
                "progress": 20,
                "message": f"订单数据读取完成，共 {len(df_order)} 行"
            })
            
            # 读取排班数据
            logger.info("正在读取排班数据...")
            schedule_buffer = BytesIO(schedule_data)
            df_schedule = pd.read_excel(schedule_buffer, engine="openpyxl")
            logger.info(f"排班数据读取成功，共 {len(df_schedule)} 行")
            
            await set_task_status(task_id, {
                "status": TASK_STATUS_PROCESSING,
                "progress": 40,
                "message": f"排班数据读取完成，共 {len(df_schedule)} 行"
            })
            
            # 验证列名
            logger.info("正在验证数据结构...")
            missing_order_columns = [col for col in required_order_columns if col not in df_order.columns]
            missing_schedule_columns = [col for col in required_schedule_columns if col not in df_schedule.columns]
            
            if missing_order_columns or missing_schedule_columns:
                error_msg = []
                if missing_order_columns:
                    error_msg.append(f"订单数据缺少列：{', '.join(missing_order_columns)}")
                if missing_schedule_columns:
                    error_msg.append(f"排班表缺少列：{', '.join(missing_schedule_columns)}")
                raise ValueError("\n".join(error_msg))
            
            await set_task_status(task_id, {
                "status": TASK_STATUS_PROCESSING,
                "progress": 60,
                "message": "数据结构验证通过，开始处理数据..."
            })
            
            # 处理数据
            logger.info("开始处理数据...")
            chunk_size = 1000  # 减小块大小以避免处理时间过长
            total_chunks = len(df_order) // chunk_size + 1
            processed_chunks = []
            
            for i in range(total_chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, len(df_order))
                chunk = df_order.iloc[start_idx:end_idx].copy()
                
                logger.info(f"处理数据块 {i+1}/{total_chunks}，行范围: {start_idx}-{end_idx}")
                
                # 更新进度
                progress = 60 + (i + 1) * 20 // total_chunks
                await set_task_status(task_id, {
                    "status": TASK_STATUS_PROCESSING,
                    "progress": progress,
                    "message": f"正在处理数据块 {i+1}/{total_chunks}..."
                })
                
                try:
                    # 处理数据块
                    processed_chunk, updated_schedule = process_order_chunk(chunk, df_schedule)
                    processed_chunks.append(processed_chunk)
                    df_schedule = updated_schedule  # 更新排班表数据
                    logger.info(f"数据块 {i+1} 处理完成")
                    
                    # 每处理完一个块就保存一次中间结果
                    temp_result = {
                        "chunk_id": i,
                        "total_chunks": total_chunks,
                        "processed_data": processed_chunk.to_dict('records')
                    }
                    await set_task_status(task_id, {
                        "status": TASK_STATUS_PROCESSING,
                        "progress": progress,
                        "message": f"正在处理数据块 {i+1}/{total_chunks}...",
                        "temp_result": temp_result
                    })
                    
                except Exception as e:
                    logger.error(f"处理数据块 {i+1} 时出错: {str(e)}")
                    raise
                
                # 释放内存并等待一小段时间
                del chunk
                await asyncio.sleep(0.1)  # 让出控制权
            
            # 合并处理后的数据并生成汇总
            logger.info("合并处理后的数据并生成汇总...")
            df_filtered = pd.concat(processed_chunks, ignore_index=True)
            df_filtered, df_schedule, df_anchor_sum, df_ck_sum = generate_summary(df_filtered, df_schedule)
            
            # 生成结果文件
            logger.info("生成结果文件...")
            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_filtered.to_excel(writer, sheet_name="主播、场控业绩筛选源表", index=False)
                df_schedule.to_excel(writer, sheet_name="主播、场控排班", index=False)
                df_anchor_sum.to_excel(writer, sheet_name="主播月总业绩汇总", index=False)
                df_ck_sum.to_excel(writer, sheet_name="场控月总业绩汇总", index=False)

            output.seek(0)
            result_data = output.getvalue()
            
            # 更新任务状态为完成
            await set_task_status(task_id, {
                "status": TASK_STATUS_COMPLETED,
                "progress": 100,
                "message": "处理完成",
                "result": base64.b64encode(result_data).decode("utf-8"),
                "completion_time": time.time()
            })
            
            # 添加处理统计信息到日志
            total_orders = len(df_filtered)
            matched_orders = len(df_filtered[df_filtered['匹配状态'] == '成功'])
            match_rate = (matched_orders / total_orders * 100) if total_orders > 0 else 0
            
            logger.info(f"任务 {task_id} 处理完成")
            logger.info(f"总订单数: {total_orders}")
            logger.info(f"成功匹配订单数: {matched_orders}")
            logger.info(f"匹配率: {match_rate:.2f}%")
            
        except Exception as e:
            logger.error(f"处理数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            raise

    except Exception as e:
        error_msg = f"任务处理失败: {str(e)}"
        logger.error(error_msg)
        logger.error(f"错误详情: {traceback.format_exc()}")
        
        # 更新任务状态为失败
        await set_task_status(task_id, {
            "status": TASK_STATUS_FAILED,
            "message": error_msg,
            "error_trace": traceback.format_exc(),
            "failure_time": time.time()
        })
        
    finally:
        # 从运行中的任务字典中移除任务
        if task_id in running_tasks:
            del running_tasks[task_id]

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

@app.websocket("/api/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket 连接处理"""
    await manager.connect(websocket)
    try:
        while True:
            # 保持连接活跃
            await websocket.receive_text()
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
    finally:
        manager.disconnect(websocket)

# 添加数据处理相关的辅助函数
def parse_datetime(date_str: str, time_str: str) -> datetime:
    """解析日期和时间字符串为datetime对象"""
    try:
        return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(f"{date_str} {time_str}", "%Y/%m/%d %H:%M:%S")
        except ValueError:
            raise ValueError(f"无法解析日期时间: {date_str} {time_str}")

def time_in_range(time_to_check: datetime, start_time: datetime, end_time: datetime) -> bool:
    """检查时间是否在指定范围内"""
    return start_time <= time_to_check <= end_time

def process_order_chunk(chunk: pd.DataFrame, df_schedule: pd.DataFrame) -> tuple:
    """按照指定逻辑处理订单数据块"""
    # 第1步：过滤数据
    # 转换数据类型
    chunk[['主订单编号', '子订单编号', '商品ID']] = chunk[['主订单编号', '子订单编号', '商品ID']].astype(str)
    
    # 关键词过滤
    keywords = ['SSS', 'DB', 'TZDN', 'DF', 'SP', 'sp', 'SC', 'sc', 'spcy']
    
    def contains_keywords(text):
        return any(kw in str(text) for kw in keywords)
    
    # 应用过滤条件
    mask_keywords = ~chunk['选购商品'].apply(contains_keywords)
    mask_alliance = ~chunk['流量来源'].str.contains('精选联盟', na=False)
    mask_content_type = (
        (chunk['流量体裁'] == '其他') & (chunk['订单应付金额'] != 0) |
        (chunk['流量体裁'] == '直播') |
        (chunk['流量体裁'] == '数据将于第二天更新')
    )
    mask_cancel = chunk['取消原因'].isna()
    
    # 合并所有过滤条件
    df_filtered = chunk[mask_keywords & mask_alliance & mask_content_type & mask_cancel].copy()
    
    # 第3步：统一转换日期/时间类型
    df_filtered['订单提交日期'] = pd.to_datetime(df_filtered['订单提交日期'], errors='coerce').dt.date
    df_filtered['订单提交时间'] = pd.to_datetime(df_filtered['订单提交时间'], format='%H:%M:%S', errors='coerce').dt.time
    
    # 处理排班表时间
    df_schedule['日期'] = pd.to_datetime(df_schedule['日期'], errors='coerce').dt.date
    df_schedule['上播时间'] = pd.to_datetime(df_schedule['上播时间'], format='%H:%M:%S', errors='coerce').dt.time
    df_schedule['下播时间'] = pd.to_datetime(df_schedule['下播时间'], format='%H:%M:%S', errors='coerce').dt.time
    
    # 第4步：匹配订单和计算GMV
    df_schedule['GMV'] = 0.0
    df_schedule['退货GMV'] = 0.0
    df_schedule['GSV'] = 0.0
    
    for i, row in df_schedule.iterrows():
        date_schedule = row['日期']
        start_time = row['上播时间']
        end_time = row['下播时间']
        
        if pd.isna(date_schedule) or pd.isna(start_time) or pd.isna(end_time):
            continue
            
        # 匹配日期和时间
        mask_date = (df_filtered['订单提交日期'] == date_schedule)
        mask_time = (
            (df_filtered['订单提交时间'] >= start_time) &
            (df_filtered['订单提交时间'] <= end_time)
        )
        
        # 计算GMV
        mask_status_GMV = df_filtered['订单状态'].isin(['已发货', '已完成', '已关闭', '待发货'])
        matched_df_GMV = df_filtered[mask_date & mask_time & mask_status_GMV]
        sum_GMV = matched_df_GMV['订单应付金额'].sum()
        df_schedule.at[i, 'GMV'] = sum_GMV
        
        # 计算退货GMV
        mask_status_refund = (df_filtered['订单状态'] == '已关闭')
        matched_df_refund = df_filtered[mask_date & mask_time & mask_status_refund]
        sum_refund = matched_df_refund['订单应付金额'].sum()
        df_schedule.at[i, '退货GMV'] = sum_refund
        
        # 计算GSV
        mask_status_GSV = df_filtered['订单状态'].isin(['已发货', '已完成', '待发货'])
        matched_df_GSV = df_filtered[mask_date & mask_time & mask_status_GSV]
        sum_GSV = matched_df_GSV['订单应付金额'].sum()
        df_schedule.at[i, 'GSV'] = sum_GSV
    
    return df_filtered, df_schedule

def generate_summary(df_filtered: pd.DataFrame, df_schedule: pd.DataFrame) -> tuple:
    """生成主播和场控的汇总统计"""
    # 按主播汇总
    cols_to_sum = ['GMV', '退货GMV', 'GSV', '时段消耗']
    
    df_anchor_sum = df_schedule.groupby('主播姓名', as_index=False)[cols_to_sum].sum()
    df_anchor_sum.rename(columns={
        'GMV': '主播GMV总和',
        '退货GMV': '主播退货GMV总和',
        'GSV': '主播GSV总和',
        '时段消耗': '总消耗'
    }, inplace=True)
    
    # 按场控汇总
    df_ck_sum = df_schedule.groupby('场控姓名', as_index=False)[cols_to_sum].sum()
    df_ck_sum.rename(columns={
        'GMV': '场控GMV总和',
        '退货GMV': '场控退货GMV总和',
        'GSV': '场控GSV总和',
        '时段消耗': '总消耗'
    }, inplace=True)
    
    return df_filtered, df_schedule, df_anchor_sum, df_ck_sum
