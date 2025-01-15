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
        <html><body><h1>Error loading template</h1></body></html>
        """

def process_excel(order_data, schedule_data):
    try:
        # 1. 读取订单数据
        df = pd.read_excel(BytesIO(order_data))
        
        # 转为字符串以防后续合并或过滤问题
        df[['主订单编号', '子订单编号', '商品ID']] = df[['主订单编号', '子订单编号', '商品ID']].astype(str)

        # 删除"选购商品"列中含有特定关键词的行
        keywords = ['SSS', 'DB', 'TZDN', 'DF', 'SP', 'sp', 'SC', 'sc', 'spcy']
        def contains_keywords(text):
            return any(kw in str(text) for kw in keywords)

        df_filtered = df[~df['选购商品'].apply(contains_keywords)]
        df_filtered = df_filtered[~df_filtered['流量来源'].str.contains('精选联盟', na=False)]
        
        # 根据"流量体裁"筛选
        df_filtered = pd.concat([
            df_filtered[(df_filtered['流量体裁'] == '其他') & (df_filtered['订单应付金额'] != 0)],
            df_filtered[df_filtered['流量体裁'] == '直播'],
            df_filtered[df_filtered['流量体裁'] == '数据将于第二天更新']
        ], ignore_index=True)

        # 筛选"取消原因"列为空
        df_filtered = df_filtered[df_filtered['取消原因'].isna()]
        
        # 2. 读取排班表
        df_schedule = pd.read_excel(BytesIO(schedule_data))
        
        # 3. 统一转换日期/时间类型
        df_filtered['订单提交日期'] = pd.to_datetime(df_filtered['订单提交日期']).dt.date
        df_schedule['日期'] = pd.to_datetime(df_schedule['日期']).dt.date
        
        for col in ['订单提交时间']:
            if col in df_filtered.columns:
                df_filtered[col] = pd.to_datetime(df_filtered[col].astype(str).str.strip()).dt.time

        for col in ['上播时间', '下播时间']:
            if col in df_schedule.columns:
                df_schedule[col] = pd.to_datetime(df_schedule[col].astype(str).str.strip()).dt.time

        # 4. 匹配并统计订单应付金额
        df_schedule['GMV'] = 0.0
        df_schedule['退货GMV'] = 0.0
        df_schedule['GSV'] = 0.0

        for i, row in df_schedule.iterrows():
            date_schedule = row['日期']
            start_time = row['上播时间']
            end_time = row['下播时间']

            if pd.isna(date_schedule) or pd.isna(start_time) or pd.isna(end_time):
                continue

            mask_date = (df_filtered['订单提交日期'] == date_schedule)
            mask_time = (
                (df_filtered['订单提交时间'] >= start_time) &
                (df_filtered['订单提交时间'] <= end_time)
            )

            # GMV
            mask_status_GMV = df_filtered['订单状态'].isin(['已发货', '已完成', '已关闭', '待发货'])
            matched_df_GMV = df_filtered[mask_date & mask_time & mask_status_GMV]
            df_schedule.at[i, 'GMV'] = matched_df_GMV['订单应付金额'].sum()

            # 退货GMV
            mask_status_refund = (df_filtered['订单状态'] == '已关闭')
            matched_df_refund = df_filtered[mask_date & mask_time & mask_status_refund]
            df_schedule.at[i, '退货GMV'] = matched_df_refund['订单应付金额'].sum()

            # GSV
            mask_status_GSV = df_filtered['订单状态'].isin(['已发货', '已完成', '待发货'])
            matched_df_GSV = df_filtered[mask_date & mask_time & mask_status_GSV]
            df_schedule.at[i, 'GSV'] = matched_df_GSV['订单应付金额'].sum()

        # 5. 汇总统计
        sum_cols = ['GMV', '退货GMV', 'GSV', '时段消耗']
        
        # 主播汇总
        if '主播姓名' in df_schedule.columns:
            df_anchor_sum = df_schedule.groupby('主播姓名')[sum_cols].sum().reset_index()
            df_anchor_sum.columns = ['主播姓名', '主播GMV总和', '主播退货GMV总和', '主播GSV总和', '总消耗']
        else:
            df_anchor_sum = pd.DataFrame()

        # 场控汇总
        if '场控姓名' in df_schedule.columns:
            df_ck_sum = df_schedule.groupby('场控姓名')[sum_cols].sum().reset_index()
            df_ck_sum.columns = ['场控姓名', '场控GMV总和', '场控退货GMV总和', '场控GSV总和', '总消耗']
        else:
            df_ck_sum = pd.DataFrame()

        # 6. 写入结果
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
        try:
            if self.path == '/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html; charset=utf-8')
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
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def do_POST(self):
        try:
            if self.path == '/api/upload':
                # 获取 Content-Type 和 boundary
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
            print(f"POST Error: {str(e)}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'success': False,
                'message': str(e)
            }).encode()) 