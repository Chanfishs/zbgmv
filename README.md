# Excel 数据处理系统

一个基于 FastAPI 和 Pandas 的 Excel 数据处理系统，用于处理订单数据和排班表，生成业绩统计报表。

## 功能特点

- 支持 .xlsx 格式的 Excel 文件上传
- 自动过滤和处理订单数据
- 匹配时间段内的订单信息
- 计算 GMV、退货 GMV 和 GSV
- 生成主播和场控的业绩汇总报表
- 异步处理大文件
- 实时进度显示
- 拖拽上传支持

## 技术栈

- 后端：Python + FastAPI + Pandas
- 前端：HTML + TailwindCSS + JavaScript
- 部署：Vercel

## 系统要求

- Python 3.8+
- Node.js 14+

## 安装部署

1. 克隆仓库
```bash
git clone https://github.com/yourusername/excel-processing-system.git
cd excel-processing-system
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

3. 本地运行
```bash
uvicorn api.process:app --reload
```

4. Vercel 部署
```bash
vercel
```

## 使用说明

1. 文件格式要求：
   - 订单数据表必须包含：主订单编号、子订单编号、商品ID等字段
   - 排班表必须包含：日期、上播时间、下播时间等字段

2. 数据处理规则：
   - 自动过滤特定关键词的商品
   - 自动匹配时间段内的订单
   - 计算 GMV、退货 GMV 和 GSV

3. 注意事项：
   - 文件大小限制：100MB
   - 仅支持 .xlsx 格式
   - 请确保数据格式正确

## 开发说明

1. 目录结构
```
.
├── api/
│   ├── process.py      # 主要处理逻辑
│   ├── templates/      # HTML 模板
│   └── ...
├── requirements.txt    # Python 依赖
└── vercel.json        # Vercel 配置
```

2. 开发模式
```bash
uvicorn api.process:app --reload --port 3000
```

## License

MIT License 