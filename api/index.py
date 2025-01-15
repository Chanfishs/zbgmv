from flask import Flask, request, jsonify, send_file, render_template
from werkzeug.utils import secure_filename
import pandas as pd
import numpy as np
import os
from datetime import datetime
import uuid
from io import BytesIO
import base64

app = Flask(__name__, template_folder='templates')

# 配置
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['ALLOWED_EXTENSIONS'] = {'xls', 'xlsx'}

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

        # 读取文件内容到内存
        order_content = BytesIO(order_file.read())
        schedule_content = BytesIO(schedule_file.read())
        
        # 处理数据
        result_filename = f"result_{uuid.uuid4()}.xlsx"
        result_content = BytesIO()
        
        process_data(order_content, schedule_content, result_content)
        
        # 将结果转换为base64
        result_content.seek(0)
        result_base64 = base64.b64encode(result_content.read()).decode('utf-8')
                
        return jsonify({
            'success': True,
            'message': '处理成功',
            'result_file': result_filename,
            'content': result_base64
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/download/<filename>')
def download_file(filename):
    try:
        # 从base64还原文件内容
        content = base64.b64decode(request.args.get('content', ''))
        
        return send_file(
            BytesIO(content),
            as_attachment=True,
            download_name=f"统计结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': '文件下载失败'
        }), 404

def process_data(order_content: BytesIO, schedule_content: BytesIO, output_content: BytesIO) -> None:
    try:
        # 1. 读取订单数据
        df = pd.read_excel(order_content)
        
        # 转为字符串以防后续合并或过滤问题
        df[['主订单编号', '子订单编号', '商品ID']] = df[['主订单编号', '子订单编号', '商品ID']].astype(str)

        # 删除"选购商品"列中含有特定关键词的行
        keywords = ['SSS', 'DB', 'TZDN', 'DF', 'SP', 'sp', 'SC', 'sc', 'spcy']
        def contains_keywords(text):
            for kw in keywords:
                if kw in str(text):
                    return True
            return False

        df_filtered = df[~df['选购商品'].apply(contains_keywords)]
        
        # 删除"流量来源"列中含有"精选联盟"的行
        df_filtered = df_filtered[~df_filtered['流量来源'].str.contains('精选联盟', na=False)]

        # 根据"流量体裁"筛选
        df_filtered_1 = df_filtered[(df_filtered['流量体裁'] == '其他') & (df_filtered['订单应付金额'] != 0)]
        df_filtered_2 = df_filtered[df_filtered['流量体裁'] == '直播']
        df_filtered_3 = df_filtered[df_filtered['流量体裁'] == '数据将于第二天更新']
        df_filtered = pd.concat([df_filtered_1, df_filtered_2, df_filtered_3], ignore_index=True)

        # 筛选"取消原因"列为空
        df_filtered = df_filtered[df_filtered['取消原因'].isna()]
        
        # 2. 读取排班表
        df_schedule = pd.read_excel(schedule_content)
        
        # 3. 统一转换日期/时间类型
        time_cols = ['订单提交时间']
        for col in time_cols:
            if col in df_filtered.columns:
                df_filtered[col] = df_filtered[col].astype(str).str.strip()

        schedule_time_cols = ['上播时间', '下播时间']
        for col in schedule_time_cols:
            if col in df_schedule.columns:
                df_schedule[col] = df_schedule[col].astype(str).str.strip()

        df_filtered['订单提交日期'] = pd.to_datetime(df_filtered['订单提交日期'], errors='coerce').dt.date
        df_schedule['日期'] = pd.to_datetime(df_schedule['日期'], errors='coerce').dt.date

        if '订单提交时间' in df_filtered.columns:
            df_filtered['订单提交时间'] = pd.to_datetime(
                df_filtered['订单提交时间'],
                format='%H:%M:%S',
                errors='coerce'
            ).dt.time

        if '上播时间' in df_schedule.columns:
            df_schedule['上播时间'] = pd.to_datetime(
                df_schedule['上播时间'],
                format='%H:%M:%S',
                errors='coerce'
            ).dt.time

        if '下播时间' in df_schedule.columns:
            df_schedule['下播时间'] = pd.to_datetime(
                df_schedule['下播时间'],
                format='%H:%M:%S',
                errors='coerce'
            ).dt.time

        # 4. 匹配并统计订单应付金额
        df_schedule['GMV'] = np.nan
        df_schedule['退货GMV'] = np.nan
        df_schedule['GSV'] = np.nan

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

            mask_status_GMV = df_filtered['订单状态'].isin(['已发货', '已完成', '已关闭', '待发货'])
            matched_df_GMV = df_filtered[mask_date & mask_time & mask_status_GMV]
            sum_GMV = matched_df_GMV['订单应付金额'].sum()
            df_schedule.at[i, 'GMV'] = sum_GMV

            mask_status_refund = (df_filtered['订单状态'] == '已关闭')
            matched_df_refund = df_filtered[mask_date & mask_time & mask_status_refund]
            sum_refund = matched_df_refund['订单应付金额'].sum()
            df_schedule.at[i, '退货GMV'] = sum_refund

            mask_status_GSV = df_filtered['订单状态'].isin(['已发货', '已完成', '待发货'])
            matched_df_GSV = df_filtered[mask_date & mask_time & mask_status_GSV]
            sum_GSV = matched_df_GSV['订单应付金额'].sum()
            df_schedule.at[i, 'GSV'] = sum_GSV

        # 5. 按主播姓名汇总
        group_col_anchor = '主播姓名'
        cols_to_sum = ['GMV', '退货GMV', 'GSV', '时段消耗']

        if all(c in df_schedule.columns for c in [group_col_anchor, '时段消耗']):
            df_anchor_sum = df_schedule.groupby(group_col_anchor, as_index=False)[cols_to_sum].sum()
            df_anchor_sum.rename(columns={
                'GMV': '主播GMV总和',
                '退货GMV': '主播退货GMV总和',
                'GSV': '主播GSV总和',
                '时段消耗': '总消耗'
            }, inplace=True)
        else:
            df_anchor_sum = pd.DataFrame()

        # 6. 按场控姓名汇总
        group_col_ck = '场控姓名'
        if all(c in df_schedule.columns for c in [group_col_ck, '时段消耗']):
            df_ck_sum = df_schedule.groupby(group_col_ck, as_index=False)[cols_to_sum].sum()
            df_ck_sum.rename(columns={
                'GMV': '场控GMV总和',
                '退货GMV': '场控退货GMV总和',
                'GSV': '场控GSV总和',
                '时段消耗': '总消耗'
            }, inplace=True)
        else:
            df_ck_sum = pd.DataFrame()

        # 7. 写入结果文件
        with pd.ExcelWriter(output_content, engine='openpyxl') as writer:
            df_filtered.to_excel(writer, sheet_name='主播、场控业绩筛选源表', index=False)
            df_schedule.to_excel(writer, sheet_name='主播、场控排班', index=False)
            if not df_anchor_sum.empty:
                df_anchor_sum.to_excel(writer, sheet_name='主播月总业绩汇总', index=False)
            if not df_ck_sum.empty:
                df_ck_sum.to_excel(writer, sheet_name='场控月总业绩汇总', index=False)
                
    except Exception as e:
        raise Exception(f"数据处理失败: {str(e)}")

# Vercel 需要的处理函数
def handler(request, context):
    with app.request_context(request):
        return app.full_dispatch_request() 