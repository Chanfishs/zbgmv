from http.server import BaseHTTPRequestHandler
import json
import pandas as pd
import numpy as np
from io import BytesIO
import base64
import os
import cgi
import tempfile

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

class Handler(BaseHTTPRequestHandler):
    def _send_cors_headers(self):
        """设置 CORS 头"""
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        """处理 OPTIONS 请求"""
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        """处理 GET 请求"""
        try:
            if self.path == '/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(read_template().encode('utf-8'))
            elif self.path in ['/favicon.ico', '/favicon.png']:
                self.send_response(204)
                self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            print(f"GET Error: {str(e)}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': str(e)
            }).encode())

    def do_POST(self):
        """处理 POST 请求"""
        try:
            if self.path == '/api/upload':
                # 获取 Content-Type
                content_type = self.headers.get('Content-Type', '')
                if not content_type.startswith('multipart/form-data'):
                    raise ValueError('Invalid content type')

                # 创建临时文件来存储上传的数据
                with tempfile.NamedTemporaryFile() as temp_file:
                    # 读取请求体
                    content_length = int(self.headers.get('Content-Length', 0))
                    temp_file.write(self.rfile.read(content_length))
                    temp_file.seek(0)

                    # 解析 multipart/form-data
                    form = cgi.FieldStorage(
                        fp=temp_file,
                        headers=self.headers,
                        environ={
                            'REQUEST_METHOD': 'POST',
                            'CONTENT_TYPE': content_type,
                        }
                    )

                    # 检查文件是否存在
                    if 'order_file' not in form or 'schedule_file' not in form:
                        raise ValueError('Missing required files')

                    # 读取文件内容
                    order_file = form['order_file']
                    schedule_file = form['schedule_file']
                    
                    # 处理数据
                    from .process import process_excel
                    result = process_excel(order_file.file.read(), schedule_file.file.read())

                    # 返回结果
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self._send_cors_headers()
                    self.end_headers()
                    response = {
                        'success': True,
                        'content': base64.b64encode(result).decode('utf-8')
                    }
                    self.wfile.write(json.dumps(response).encode())
            else:
                self.send_response(404)
                self.end_headers()

        except Exception as e:
            print(f"POST Error: {str(e)}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({
                'success': False,
                'message': str(e)
            }).encode())

def handler(request, response):
    """Vercel 入口函数"""
    return Handler.handler(request, response) 