from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os

from .process import app as process_app

app = FastAPI()

# 添加 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 挂载 process 应用的路由
app.mount("/api", process_app)

# 读取 HTML 模板
def read_template():
    try:
        template_path = os.path.join(os.path.dirname(__file__), 'templates/index.html')
        with open(template_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading template: {str(e)}")
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Error</title>
            <meta charset="utf-8">
        </head>
        <body>
            <h1>Error loading template</h1>
            <p>Please check server logs</p>
        </body>
        </html>
        """

@app.get("/", response_class=HTMLResponse)
async def read_root():
    return read_template()

@app.get("/favicon.ico")
async def favicon():
    return {"status": "ok"} 