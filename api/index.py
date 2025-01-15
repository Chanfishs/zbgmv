from flask import Flask, request, jsonify, send_file, render_template
from werkzeug.utils import secure_filename
import pandas as pd
import numpy as np
import os
from datetime import datetime
import uuid
from tempfile import NamedTemporaryFile

app = Flask(__name__)

# 配置
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['UPLOAD_FOLDER'] = '/tmp/uploads'
app.config['RESULT_FOLDER'] = '/tmp/results'
app.config['ALLOWED_EXTENSIONS'] = {'xls', 'xlsx'}

# 确保临时目录存在
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULT_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/api/upload', methods=['POST'])
def upload_files():
    try:
        if 'order_file' not in request.files or 'schedule_file' not in request.files:
            return jsonify({'success': False, 'message': '请同时上传订单文件和排班表文件'}), 400
            
        order_file = request.files['order_file']
        schedule_file = request.files['schedule_file']
        
        if order_file.filename == '' or schedule_file.filename == '':
            return jsonify({'success': False, 'message': '请选择有效的文件'}), 400
            
        if not all(allowed_file(f.filename) for f in [order_file, schedule_file]):
            return jsonify({'success': False, 'message': '不支持的文件类型'}), 400

        # 使用临时文件
        order_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(order_file.filename))
        schedule_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(schedule_file.filename))
        
        order_file.save(order_path)
        schedule_file.save(schedule_path)
        
        # 处理数据
        result_filename = f"result_{uuid.uuid4()}.xlsx"
        result_path = os.path.join(app.config['RESULT_FOLDER'], result_filename)
        
        process_data(order_path, schedule_path, result_path)
        
        # 清理临时文件
        os.remove(order_path)
        os.remove(schedule_path)
        
        return jsonify({
            'success': True,
            'message': '处理成功',
            'result_file': result_filename
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/download/<filename>')
def download_file(filename):
    try:
        return send_file(
            os.path.join(app.config['RESULT_FOLDER'], filename),
            as_attachment=True,
            download_name=f"统计结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
    except Exception as e:
        return jsonify({
            'success': False,
            'message': '文件下载失败'
        }), 404

# 这里添加 process_data 函数的实现...

# Vercel 需要的处理函数
def handler(request, context):
    return app(request) 