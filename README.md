# Excel 数据处理 API

这是一个基于 FastAPI 的 Excel 数据处理 API，用于处理订单数据和排班表数据。

## 项目结构

```
.
├── api/
│   └── process.py      # API 主要处理逻辑
├── test_api.py         # API 测试脚本
├── requirements.txt    # 项目依赖
├── vercel.json        # Vercel 部署配置
└── README.md          # 项目说明文档
```

## 功能特点

- 支持上传订单数据和排班表
- 自动处理数据匹配和统计
- 生成多个数据汇总表
- 支持 Excel 文件导出

## API 端点

### GET /
- 显示欢迎信息
- 返回：`{"message": "Welcome to the Excel Processing API. Please use POST /api/process to upload files."}`

### POST /api/process
- 上传并处理 Excel 文件
- 参数：
  - `order_file`: 订单数据文件 (Excel)
  - `schedule_file`: 排班表文件 (Excel)
- 返回：处理后的 Excel 文件

## 本地测试

1. 安装依赖：
```bash
pip install -r requirements.txt
```

2. 准备测试文件：
- `订单数据.xlsx`
- `排班表.xlsx`

3. 运行测试脚本：
```bash
python test_api.py
```

## 在线访问

API 已部署到 Vercel，可以通过以下地址访问：
- https://zbgmv-p9tx.vercel.app/

## 技术栈

- FastAPI
- Pandas
- OpenPyXL
- NumPy
- Python 3.11

## 注意事项

1. Excel 文件格式要求：
   - 订单数据表必须包含：主订单编号、子订单编号、商品ID等字段
   - 排班表必须包含：日期、上播时间、下播时间等字段

2. 数据处理规则：
   - 自动过滤特定关键词的商品
   - 自动匹配时间段内的订单
   - 计算 GMV、退货 GMV 和 GSV

## 开发说明

1. 本地开发：
```bash
uvicorn api.process:app --reload
```

2. 部署更新：
```bash
git add .
git commit -m "更新说明"
git push origin main
```

## 错误处理

API 会返回适当的错误信息和状态码：
- 200: 成功
- 400: 请求错误（如文件格式不正确）
- 500: 服务器处理错误 