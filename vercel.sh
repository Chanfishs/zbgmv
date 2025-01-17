#!/bin/bash

# 安装系统依赖
apt-get update
apt-get install -y $(cat vercel_requirements.txt)

# 安装 Python 依赖
pip install -r requirements.txt 