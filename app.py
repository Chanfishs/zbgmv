import os
import uuid
from typing import Set, List, Tuple
from flask import Flask, request, jsonify, current_app, send_file, render_template
from werkzeug.utils import secure_filename
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from flask_cors import CORS
import numpy as np

# 配置类移到顶部
class Config:
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 限制上传文件大小为16MB
    UPLOAD_FOLDER = 'uploads'
    RESULT_FOLDER = 'results'
    ALLOWED_EXTENSIONS = {'xls', 'xlsx'}
    LOG_FILE = 'app.log'

app = Flask(__name__)
app.config.from_object(Config)  # 加载配置
CORS(app)

# 确保上传和结果目录存在
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULT_FOLDER'], exist_ok=True)

def setup_logger():
    """配置日志"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    log_file = os.path.join('logs', app.config['LOG_FILE'])
    handler = RotatingFileHandler(log_file, maxBytes=10000, backupCount=3)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.INFO)
    
    # 记录应用启动日志
    app.logger.info('应用启动')

class ExcelProcessError(Exception):
    """Excel处理相关的自定义异常"""
    pass

def allowed_file(filename: str) -> bool:
    """
    验证文件扩展名是否允许
    
    Args:
        filename: 文件名
        
    Returns:
        bool: 是否是允许的文件类型
    """
    if not filename:
        return False
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def validate_excel_columns(df: pd.DataFrame, required_columns: List[str]) -> Tuple[bool, List[str]]:
    """
    验证DataFrame是否包含所需列
    
    Args:
        df: pandas DataFrame对象
        required_columns: 必需的列名列表
        
    Returns:
        Tuple[bool, List[str]]: (是否验证通过, 缺失的列名列表)
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    return len(missing_columns) == 0, missing_columns

def save_upload_file(file, folder: str) -> str:
    """
    安全地保存上传文件
    
    Args:
        file: 文件对象
        folder: 保存目录
        
    Returns:
        str: 保存后的文件路径
    """
    filename = f"{uuid.uuid4()}_{secure_filename(file.filename)}"
    filepath = os.path.join(folder, filename)
    file.save(filepath)
    return filepath

@app.route('/upload', methods=['POST'])
def upload_files():
    """处理文件上传请求"""
    try:
        # 获取上传文件
        if 'order_file' not in request.files or 'schedule_file' not in request.files:
            app.logger.error("文件未上传")
            return jsonify({
                'success': False,
                'message': '请同时上传订单文件和排班表文件'
            }), 400
            
        order_file = request.files['order_file']
        schedule_file = request.files['schedule_file']
        
        # 验证文件名不为空
        if order_file.filename == '' or schedule_file.filename == '':
            app.logger.error("文件名为空")
            return jsonify({
                'success': False,
                'message': '请选择有效的文件'
            }), 400
            
        # 验证文件类型
        for file in (order_file, schedule_file):
            if not allowed_file(file.filename):
                app.logger.error(f"不支持的文件类型: {file.filename}")
                return jsonify({
                    'success': False,
                    'message': f'不支持的文件类型: {file.filename}'
                }), 400
        
        # 确保目录存在
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        os.makedirs(app.config['RESULT_FOLDER'], exist_ok=True)
        
        try:
            # 保存文件
            order_path = save_upload_file(order_file, app.config['UPLOAD_FOLDER'])
            schedule_path = save_upload_file(schedule_file, app.config['UPLOAD_FOLDER'])
            
            # 生成结果文件路径
            result_filename = f"result_{uuid.uuid4()}.xlsx"
            result_path = os.path.join(app.config['RESULT_FOLDER'], result_filename)
            
            # 处理数据
            process_data(order_path, schedule_path, result_path)
            
            # 清理临时文件
            try:
                os.remove(order_path)
                os.remove(schedule_path)
            except Exception as e:
                app.logger.error(f"清理临时文件失败: {str(e)}")
            
            return jsonify({
                'success': True,
                'message': '处理成功',
                'result_file': result_filename
            })
            
        except Exception as e:
            app.logger.error(f"处理文件时发生错误: {str(e)}")
            # 尝试清理临时文件
            for path in [order_path, schedule_path]:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except:
                    pass
            raise
            
    except ExcelProcessError as e:
        app.logger.error(f"Excel处理错误: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Excel处理错误: {str(e)}'
        }), 400
    except Exception as e:
        app.logger.error(f"服务器内部错误: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'服务器内部错误: {str(e)}'
        }), 500

@app.route('/')
def index():
    """渲染首页"""
    return render_template('index.html')

def cleanup_old_files():
    """清理超过24小时的临时文件"""
    try:
        current_time = datetime.now()
        # 清理上传目录
        for filename in os.listdir(app.config['UPLOAD_FOLDER']):
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file_time = datetime.fromtimestamp(os.path.getctime(filepath))
            if (current_time - file_time).days >= 1:
                os.remove(filepath)
                app.logger.info(f"已清理上传文件: {filename}")
                
        # 清理结果目录
        for filename in os.listdir(app.config['RESULT_FOLDER']):
            filepath = os.path.join(app.config['RESULT_FOLDER'], filename)
            file_time = datetime.fromtimestamp(os.path.getctime(filepath))
            if (current_time - file_time).days >= 1:
                os.remove(filepath)
                app.logger.info(f"已清理结果文件: {filename}")
    except Exception as e:
        app.logger.error(f"清理文件失败: {str(e)}")

def process_data(order_file_path: str, schedule_file_path: str, output_file_path: str) -> None:
    try:
        current_app.logger.info(f"开始处理数据... 订单文件: {order_file_path}, 排班表: {schedule_file_path}")
        
        # 1. 读取订单数据
        df = pd.read_excel(order_file_path)
        
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

        print("过滤后 df_filtered 行数:", len(df_filtered))
        if df_filtered.empty:
            print("警告：过滤后没有任何数据，请检查过滤条件是否过于严格。")
            
        # 2. 读取排班表
        df_schedule = pd.read_excel(schedule_file_path)
        
        # 3. 统一转换日期/时间类型
        # 先把时间列转为字符串并strip空格
        time_cols = ['订单提交时间']
        for col in time_cols:
            if col in df_filtered.columns:
                df_filtered[col] = df_filtered[col].astype(str).str.strip()

        # 对主播排班表的上播/下播时间做同样处理
        schedule_time_cols = ['上播时间', '下播时间']
        for col in schedule_time_cols:
            if col in df_schedule.columns:
                df_schedule[col] = df_schedule[col].astype(str).str.strip()

        # 转换日期列
        df_filtered['订单提交日期'] = pd.to_datetime(df_filtered['订单提交日期'], errors='coerce').dt.date
        df_schedule['日期'] = pd.to_datetime(df_schedule['日期'], errors='coerce').dt.date

        # 转换时间列
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

            # 先匹配日期
            mask_date = (df_filtered['订单提交日期'] == date_schedule)

            # 再匹配时间区间
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

        # 5. 按主播姓名汇总
        group_col_anchor = '主播姓名'
        cols_to_sum = ['GMV', '退货GMV', 'GSV', '时段消耗']

        for col_check in [group_col_anchor, '时段消耗']:
            if col_check not in df_schedule.columns:
                current_app.logger.warning(f"排班表中未找到列：{col_check}")

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
        if group_col_ck not in df_schedule.columns:
            current_app.logger.warning(f"排班表中未找到列：{group_col_ck}")

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
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            df_filtered.to_excel(writer, sheet_name='主播、场控业绩筛选源表', index=False)
            df_schedule.to_excel(writer, sheet_name='主播、场控排班', index=False)
            if not df_anchor_sum.empty:
                df_anchor_sum.to_excel(writer, sheet_name='主播月总业绩汇总', index=False)
            if not df_ck_sum.empty:
                df_ck_sum.to_excel(writer, sheet_name='场控月总业绩汇总', index=False)

        current_app.logger.info("数据处理完成")
        
    except Exception as e:
        current_app.logger.error(f"数据处理失败: {str(e)}")
        raise

@app.route('/download/<filename>')
def download_file(filename):
    """提供结果文件下载"""
    try:
        return send_file(
            os.path.join(app.config['RESULT_FOLDER'], filename),
            as_attachment=True,
            download_name=f"统计结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
    except Exception as e:
        app.logger.error(f"下载文件失败: {str(e)}")
        return jsonify({
            'success': False,
            'message': '文件下载失败'
        }), 404

if __name__ == '__main__':
    # 设置定时清理任务
    from apscheduler.schedulers.background import BackgroundScheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(cleanup_old_files, 'interval', hours=1)
    scheduler.start()
    
    setup_logger()
    app.run(debug=True) 