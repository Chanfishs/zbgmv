# Excel 数据处理系统

一个基于 FastAPI 和 Python 的 Excel 数据处理系统，专门用于处理订单数据和排班表，生成业绩统计报表。

## 功能特点

- 支持 Excel (.xlsx) 文件上传和处理
- 自动过滤和匹配订单数据
- 计算 GMV、退货 GMV 和 GSV
- 生成主播和场控的业绩汇总报表
- 实时处理进度显示
- 支持文件拖拽上传
- 美观的用户界面

## 技术栈

- 后端：FastAPI + Python
- 前端：HTML + TailwindCSS + JavaScript
- 数据处理：Pandas + NumPy
- 部署：Vercel

## 系统要求

- Python 3.8+
- FastAPI
- pandas
- numpy
- openpyxl

## 安装说明

1. 克隆仓库
```bash
git clone https://github.com/yourusername/excel-processing-system.git
cd excel-processing-system
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

3. 运行开发服务器
```bash
uvicorn api.process:app --reload
```

## 使用说明

1. 访问系统首页
2. 上传订单数据文件和排班表文件（.xlsx 格式）
3. 点击"开始处理"按钮
4. 等待处理完成，系统会自动下载处理结果

### 文件格式要求

#### 订单数据表必须包含以下字段：
- 主订单编号
- 子订单编号
- 商品ID
- 选购商品
- 流量来源
- 流量体裁
- 取消原因
- 订单状态
- 订单应付金额
- 订单提交日期
- 订单提交时间

#### 排班表必须包含以下字段：
- 日期
- 上播时间
- 下播时间
- 主播姓名
- 场控姓名
- 时段消耗

## 注意事项

- 文件大小限制：100MB
- 仅支持 .xlsx 格式的 Excel 文件
- 请确保数据格式正确，避免空值和错误格式
- 处理大文件可能需要较长时间，请耐心等待

## 许可证

MIT License 