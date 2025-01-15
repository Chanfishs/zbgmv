from fastapi import FastAPI, UploadFile, File
from fastapi.responses import Response, HTMLResponse, JSONResponse
import pandas as pd
import numpy as np
from io import BytesIO

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def root():
    html_content = """
    <!DOCTYPE html>
    <html lang="zh">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Excel 数据处理 API</title>
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
        <style>
            .error-message {
                display: none;
                margin-top: 1rem;
                padding: 1rem;
                border-radius: 0.375rem;
                background-color: #FEE2E2;
                border: 1px solid #F87171;
                color: #B91C1C;
            }
            .loading {
                display: none;
                margin-top: 1rem;
                text-align: center;
                color: #3B82F6;
            }
        </style>
    </head>
    <body class="bg-gray-100">
        <div class="container mx-auto px-4 py-8">
            <div class="max-w-2xl mx-auto bg-white rounded-lg shadow-lg p-6">
                <h1 class="text-3xl font-bold text-center mb-8">Excel 数据处理 API</h1>
                
                <div class="mb-8">
                    <h2 class="text-xl font-semibold mb-4">文件上传</h2>
                    <form id="uploadForm" class="space-y-4">
                        <div>
                            <label class="block text-sm font-medium text-gray-700">订单数据文件</label>
                            <input type="file" name="order_file" accept=".xlsx" required
                                   class="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm"
                                   onchange="validateFile(this)">
                        </div>
                        <div>
                            <label class="block text-sm font-medium text-gray-700">排班表文件</label>
                            <input type="file" name="schedule_file" accept=".xlsx" required
                                   class="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm"
                                   onchange="validateFile(this)">
                        </div>
                        <button type="submit" id="submitBtn"
                                class="w-full bg-blue-500 text-white px-4 py-2 rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-500">
                            处理数据
                        </button>
                    </form>
                    <div id="errorMessage" class="error-message"></div>
                    <div id="loading" class="loading">
                        <svg class="animate-spin h-5 w-5 mr-3 inline-block" viewBox="0 0 24 24">
                            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" fill="none"></circle>
                            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                        </svg>
                        正在处理数据，请稍候...
                    </div>
                </div>

                <div class="space-y-4">
                    <h2 class="text-xl font-semibold">使用说明</h2>
                    <div class="prose">
                        <h3 class="text-lg font-medium">Excel 文件格式要求：</h3>
                        <ul class="list-disc pl-5 space-y-2">
                            <li>订单数据表必须包含：主订单编号、子订单编号、商品ID等字段</li>
                            <li>排班表必须包含：日期、上播时间、下播时间等字段</li>
                        </ul>

                        <h3 class="text-lg font-medium mt-4">数据处理规则：</h3>
                        <ul class="list-disc pl-5 space-y-2">
                            <li>自动过滤特定关键词的商品</li>
                            <li>自动匹配时间段内的订单</li>
                            <li>计算 GMV、退货 GMV 和 GSV</li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>

        <script>
            const form = document.getElementById('uploadForm');
            const submitBtn = document.getElementById('submitBtn');
            const errorMessage = document.getElementById('errorMessage');
            const loading = document.getElementById('loading');

            function validateFile(input) {
                const file = input.files[0];
                if (file) {
                    if (!file.name.endsWith('.xlsx')) {
                        errorMessage.textContent = '请上传 .xlsx 格式的文件';
                        errorMessage.style.display = 'block';
                        input.value = '';
                        return false;
                    }
                    if (file.size > 10 * 1024 * 1024) {  // 10MB
                        errorMessage.textContent = '文件大小不能超过 10MB';
                        errorMessage.style.display = 'block';
                        input.value = '';
                        return false;
                    }
                    errorMessage.style.display = 'none';
                    return true;
                }
                return false;
            }

            form.addEventListener('submit', async (e) => {
                e.preventDefault();
                
                // 重置状态
                errorMessage.style.display = 'none';
                errorMessage.textContent = '';
                loading.style.display = 'block';
                submitBtn.disabled = true;
                
                const formData = new FormData();
                const orderFile = document.querySelector('input[name="order_file"]').files[0];
                const scheduleFile = document.querySelector('input[name="schedule_file"]').files[0];
                
                if (!orderFile || !scheduleFile) {
                    errorMessage.textContent = '请选择所有必需的文件';
                    errorMessage.style.display = 'block';
                    loading.style.display = 'none';
                    submitBtn.disabled = false;
                    return;
                }
                
                formData.append('order_file', orderFile);
                formData.append('schedule_file', scheduleFile);
                
                try {
                    const response = await fetch('/api/process', {
                        method: 'POST',
                        body: formData
                    });
                    
                    const contentType = response.headers.get('content-type');
                    
                    if (response.ok && contentType && contentType.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')) {
                        const blob = await response.blob();
                        const url = window.URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = '处理结果.xlsx';
                        document.body.appendChild(a);
                        a.click();
                        window.URL.revokeObjectURL(url);
                        a.remove();
                    } else {
                        const data = await response.json();
                        errorMessage.textContent = data.error || '处理失败，请检查文件格式是否正确';
                        errorMessage.style.display = 'block';
                    }
                } catch (error) {
                    errorMessage.textContent = '上传失败：' + error.message;
                    errorMessage.style.display = 'block';
                } finally {
                    loading.style.display = 'none';
                    submitBtn.disabled = false;
                }
            });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.post("/api/process")
async def handle_upload(order_file: UploadFile = File(...), schedule_file: UploadFile = File(...)):
    try:
        print(f"开始处理文件上传...")
        print(f"订单文件名: {order_file.filename}")
        print(f"排班表文件名: {schedule_file.filename}")
        
        # 验证文件扩展名
        if not order_file.filename.endswith('.xlsx') or not schedule_file.filename.endswith('.xlsx'):
            print("文件格式错误：非 .xlsx 格式")
            return JSONResponse(
                status_code=400,
                content={"error": "文件格式错误：请上传 .xlsx 格式的文件"}
            )

        # 读取文件内容
        try:
            print("正在读取订单文件...")
            order_data = await order_file.read()
            print(f"订单文件大小: {len(order_data)} bytes")
            
            print("正在读取排班表文件...")
            schedule_data = await schedule_file.read()
            print(f"排班表文件大小: {len(schedule_data)} bytes")
        except Exception as e:
            print(f"文件读取失败: {str(e)}")
            return JSONResponse(
                status_code=400,
                content={"error": f"文件读取失败：{str(e)}"}
            )

        # 验证文件大小
        MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
        if len(order_data) > MAX_FILE_SIZE or len(schedule_data) > MAX_FILE_SIZE:
            return JSONResponse(
                status_code=400,
                content={"error": "文件过大：请确保每个文件小于100MB"}
            )

        # 验证文件是否为空
        if len(order_data) == 0 or len(schedule_data) == 0:
            print("文件内容为空")
            return JSONResponse(
                status_code=400,
                content={"error": "文件内容为空"}
            )

        # 尝试预览文件内容
        try:
            print("尝试预览订单文件内容...")
            df_preview = pd.read_excel(BytesIO(order_data), nrows=1)
            print(f"订单文件列名: {list(df_preview.columns)}")
            
            print("尝试预览排班表内容...")
            schedule_preview = pd.read_excel(BytesIO(schedule_data), nrows=1)
            print(f"排班表列名: {list(schedule_preview.columns)}")
        except Exception as e:
            print(f"文件预览失败: {str(e)}")
            return JSONResponse(
                status_code=400,
                content={"error": f"文件格式错误：无法读取文件内容，请确保文件为有效的 Excel 文件。详细错误：{str(e)}"}
            )

        # 处理数据
        try:
            print("开始处理数据...")
            result = process_excel(order_data, schedule_data)
            print("数据处理完成，准备返回结果")
            return Response(
                content=result,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": "attachment; filename=processed_result.xlsx"
                }
            )
        except Exception as e:
            error_msg = str(e)
            print(f"数据处理失败: {error_msg}")
            if "KeyError" in error_msg:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"文件格式错误：缺少必要的列 {error_msg}"}
                )
            elif "ValueError" in error_msg:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"数据格式错误：{error_msg}"}
                )
            else:
                return JSONResponse(
                    status_code=500,
                    content={"error": f"处理失败：{error_msg}"}
                )
    except Exception as e:
        print(f"系统错误: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"系统错误：{str(e)}"}
        )

def process_excel(order_data, schedule_data):
    """处理 Excel 数据的核心逻辑"""
    try:
        # 验证必要的列是否存在
        required_order_columns = ['主订单编号', '子订单编号', '商品ID', '选购商品', '流量来源', 
                                '流量体裁', '取消原因', '订单状态', '订单应付金额', 
                                '订单提交日期', '订单提交时间']
        required_schedule_columns = ['日期', '上播时间', '下播时间', '主播姓名', '场控姓名', '时段消耗']

        # ========== 第 1 步：读取并验证原始数据 ==========
        print("正在读取原始订单数据...")
        try:
            df = pd.read_excel(BytesIO(order_data))
        except Exception as e:
            raise Exception(f"订单数据文件读取失败：{str(e)}")

        # 验证订单数据列
        missing_columns = [col for col in required_order_columns if col not in df.columns]
        if missing_columns:
            raise Exception(f"订单数据缺少必要的列：{', '.join(missing_columns)}")

        # 转为字符串以防后续合并或过滤问题
        try:
            df[['主订单编号', '子订单编号', '商品ID']] = df[['主订单编号', '子订单编号', '商品ID']].astype(str)
        except Exception as e:
            raise Exception(f"订单编号或商品ID格式转换失败：{str(e)}")

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
            raise Exception("过滤后没有任何数据，请检查过滤条件是否过于严格或数据是否符合要求")

        # ========== 第 2 步：读取并验证排班表 ==========
        print("正在读取主播排班数据...")
        try:
            df_schedule = pd.read_excel(BytesIO(schedule_data))
        except Exception as e:
            raise Exception(f"排班表文件读取失败：{str(e)}")

        # 验证排班表列
        missing_columns = [col for col in required_schedule_columns if col not in df_schedule.columns]
        if missing_columns:
            raise Exception(f"排班表缺少必要的列：{', '.join(missing_columns)}")

        # ========== 第 3 步：统一转换日期/时间类型 ==========
        # 先尝试把这几列都转为字符串并 strip 空格，以排除 Excel 中出现的隐藏字符
        time_cols = ['订单提交时间']
        for col in time_cols:
            if col in df_filtered.columns:
                df_filtered[col] = df_filtered[col].astype(str).str.strip()

        # 对主播排班表的上播/下播时间做同样处理
        schedule_time_cols = ['上播时间', '下播时间']
        for col in schedule_time_cols:
            if col in df_schedule.columns:
                df_schedule[col] = df_schedule[col].astype(str).str.strip()

        # 让 pandas 自动解析日期/时间格式
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

        # 检查日期时间转换后的空值
        date_time_errors = []
        if df_filtered['订单提交日期'].isna().any():
            date_time_errors.append("订单数据中存在无效的日期格式")
        if df_filtered['订单提交时间'].isna().any():
            date_time_errors.append("订单数据中存在无效的时间格式")
        if df_schedule['日期'].isna().any():
            date_time_errors.append("排班表中存在无效的日期格式")
        if df_schedule['上播时间'].isna().any():
            date_time_errors.append("排班表中存在无效的上播时间格式")
        if df_schedule['下播时间'].isna().any():
            date_time_errors.append("排班表中存在无效的下播时间格式")

        if date_time_errors:
            raise Exception("日期时间格式错误：" + "；".join(date_time_errors))

        # ========== 第 4 步：匹配并统计"订单应付金额" ==========
        df_schedule['GMV'] = np.nan
        df_schedule['退货GMV'] = np.nan
        df_schedule['GSV'] = np.nan

        print("开始根据日期、时间匹配订单数据...")
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

            # （A）计算 GMV
            mask_status_GMV = df_filtered['订单状态'].isin(['已发货', '已完成', '已关闭', '待发货'])
            matched_df_GMV = df_filtered[mask_date & mask_time & mask_status_GMV]
            sum_GMV = matched_df_GMV['订单应付金额'].sum()
            df_schedule.at[i, 'GMV'] = sum_GMV

            # （B）计算退货GMV
            mask_status_refund = (df_filtered['订单状态'] == '已关闭')
            matched_df_refund = df_filtered[mask_date & mask_time & mask_status_refund]
            sum_refund = matched_df_refund['订单应付金额'].sum()
            df_schedule.at[i, '退货GMV'] = sum_refund

            # （C）计算 GSV
            mask_status_GSV = df_filtered['订单状态'].isin(['已发货', '已完成', '待发货'])
            matched_df_GSV = df_filtered[mask_date & mask_time & mask_status_GSV]
            sum_GSV = matched_df_GSV['订单应付金额'].sum()
            df_schedule.at[i, 'GSV'] = sum_GSV

        # ========== 第 5 步：按「主播姓名」汇总 ==========
        group_col_anchor = '主播姓名'
        cols_to_sum = ['GMV', '退货GMV', 'GSV', '时段消耗']

        # 检查列是否存在
        for col_check in [group_col_anchor, '时段消耗']:
            if col_check not in df_schedule.columns:
                print(f"警告：排班表中未找到列：{col_check}，请检查列名。")

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

        # ========== 第 6 步：按「场控姓名」汇总 ==========
        group_col_ck = '场控姓名'
        if group_col_ck not in df_schedule.columns:
            print(f"警告：排班表中未找到列：{group_col_ck}，请检查列名。")

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

        # ========== 第 7 步：写入结果 ==========
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