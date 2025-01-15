from http.server import BaseHTTPRequestHandler
from flask import Flask, request, jsonify, send_file
import pandas as pd
import numpy as np
from io import BytesIO
import base64
import json
import cgi

app = Flask(__name__)

def process_excel(order_data, schedule_data):
    try:
        # 1. 读取订单数据
        df = pd.read_excel(BytesIO(order_data))
        df_schedule = pd.read_excel(BytesIO(schedule_data))
        
        # ... 其他数据处理逻辑保持不变 ...
        
        # 写入结果到内存
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
        raise Exception(f"数据处理失败: {str(e)}")

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            with open('api/templates/index.html', 'rb') as f:
                self.wfile.write(f.read())
        else:
            self.send_response(404)
            self.end_headers()
            
    def do_POST(self):
        try:
            if self.path == '/api/upload':
                # 解析 multipart/form-data
                content_type = self.headers.get('Content-Type', '')
                if not content_type.startswith('multipart/form-data'):
                    raise ValueError('Invalid content type')
                
                form = cgi.FieldStorage(
                    fp=self.rfile,
                    headers=self.headers,
                    environ={'REQUEST_METHOD': 'POST'}
                )
                
                order_file = form['order_file']
                schedule_file = form['schedule_file']
                
                # 处理数据
                result = process_excel(order_file.file.read(), schedule_file.file.read())
                
                # 返回结果
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
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
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                'success': False,
                'message': str(e)
            }
            self.wfile.write(json.dumps(response).encode()) 