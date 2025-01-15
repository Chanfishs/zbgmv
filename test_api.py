import requests
import os
from datetime import datetime

def test_api():
    # API 地址
    url = "https://zbgmv-p9tx.vercel.app/api/process"
    
    # 检查上传文件是否存在
    order_file = "订单数据.xlsx"
    schedule_file = "排班表.xlsx"
    
    if not os.path.exists(order_file) or not os.path.exists(schedule_file):
        print(f"错误: 请确保 {order_file} 和 {schedule_file} 文件存在于当前目录")
        return
    
    try:
        # 准备文件
        files = {
            'order_file': ('订单数据.xlsx', open(order_file, 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
            'schedule_file': ('排班表.xlsx', open(schedule_file, 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        }
        
        print("正在上传文件...")
        response = requests.post(url, files=files)
        
        # 检查响应
        if response.status_code == 200:
            # 生成带时间戳的文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"处理结果_{timestamp}.xlsx"
            
            # 保存响应内容
            with open(output_file, 'wb') as f:
                f.write(response.content)
            print(f"成功! 结果已保存到: {output_file}")
        else:
            print(f"错误: API 返回状态码 {response.status_code}")
            print(f"错误信息: {response.text}")
    
    except Exception as e:
        print(f"发生错误: {str(e)}")
    
    finally:
        # 关闭文件
        for file in files.values():
            file[1].close()

if __name__ == "__main__":
    test_api() 