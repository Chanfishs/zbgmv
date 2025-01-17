#!/bin/bash

# 更新包列表
apt-get update

# 安装系统依赖
xargs apt-get install -y < vercel_requirements.txt

# 安装 Python 依赖
pip install -r requirements.txt 