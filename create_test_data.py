import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time

# 生成订单数据
def create_order_data():
    print("开始生成订单数据...")
    
    # 生成基础数据
    n_orders = 1000
    dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
    
    data = {
        '主订单编号': [f'MO{i:06d}' for i in range(n_orders)],
        '子订单编号': [f'SO{i:06d}' for i in range(n_orders)],
        '商品ID': [f'P{i:06d}' for i in range(n_orders)],
        '选购商品': [f'普通商品{i}' for i in range(n_orders)],  # 确保不包含特殊关键词
        '流量来源': np.random.choice(['直播间', '短视频', '其他'], n_orders, p=[0.6, 0.2, 0.2]),  # 调整概率分布
        '流量体裁': np.random.choice(['直播', '其他', '数据将于第二天更新'], n_orders, p=[0.7, 0.2, 0.1]),
        '取消原因': [None] * n_orders,  # 全部为空
        '订单状态': np.random.choice(['已发货', '已完成', '待发货', '已关闭'], n_orders, p=[0.4, 0.3, 0.2, 0.1]),
        '订单应付金额': np.random.randint(100, 10000, n_orders),  # 调整金额范围
        '订单提交日期': np.random.choice(dates, n_orders),
        '订单提交时间': [f"{np.random.randint(9, 22):02d}:{np.random.randint(0, 59):02d}:00" for _ in range(n_orders)]  # 调整时间范围
    }
    
    df_orders = pd.DataFrame(data)
    
    # 确保日期和时间格式正确
    df_orders['订单提交日期'] = pd.to_datetime(df_orders['订单提交日期']).dt.date
    df_orders['订单提交时间'] = pd.to_datetime(df_orders['订单提交时间']).dt.time
    
    print(f"订单数据生成完成，共 {len(df_orders)} 条记录")
    return df_orders

# 生成排班表数据
def create_schedule_data():
    print("开始生成排班表数据...")
    
    # 生成基础数据
    dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
    anchors = ['主播A', '主播B', '主播C', '主播D']  # 增加主播数量
    controllers = ['场控X', '场控Y', '场控Z']
    
    schedules = []
    for date in dates:
        for anchor in anchors:
            # 每个主播每天2-3个时段
            n_slots = np.random.randint(2, 4)
            # 确保时间段不重叠
            time_slots = []
            for _ in range(n_slots):
                while True:
                    start_hour = np.random.randint(9, 20)
                    duration = np.random.randint(2, 4)  # 2-3小时
                    end_hour = start_hour + duration
                    
                    # 检查是否与现有时间段重叠
                    overlap = False
                    for existing_slot in time_slots:
                        if (start_hour >= existing_slot[0] and start_hour < existing_slot[1]) or \
                           (end_hour > existing_slot[0] and end_hour <= existing_slot[1]):
                            overlap = True
                            break
                    
                    if not overlap and end_hour <= 22:  # 确保不超过22点
                        time_slots.append((start_hour, end_hour))
                        break
                
                schedule = {
                    '日期': date.date(),
                    '上播时间': f"{start_hour:02d}:00:00",
                    '下播时间': f"{end_hour:02d}:00:00",
                    '主播姓名': anchor,
                    '场控姓名': np.random.choice(controllers),
                    '时段消耗': np.random.randint(1000, 5000)  # 调整消耗范围
                }
                schedules.append(schedule)
    
    df_schedule = pd.DataFrame(schedules)
    
    # 确保日期和时间格式正确
    df_schedule['日期'] = pd.to_datetime(df_schedule['日期']).dt.date
    df_schedule['上播时间'] = pd.to_datetime(df_schedule['上播时间']).dt.time
    df_schedule['下播时间'] = pd.to_datetime(df_schedule['下播时间']).dt.time
    
    print(f"排班表数据生成完成，共 {len(df_schedule)} 条记录")
    return df_schedule

# 生成测试文件
def create_test_files():
    print("开始生成测试文件...")
    
    try:
        # 生成订单数据
        df_orders = create_order_data()
        df_orders.to_excel('test_order.xlsx', index=False)
        print("订单数据文件已保存为 test_order.xlsx")
        
        # 生成排班表数据
        df_schedule = create_schedule_data()
        df_schedule.to_excel('test_schedule.xlsx', index=False)
        print("排班表数据文件已保存为 test_schedule.xlsx")
        
        # 验证生成的文件
        print("\n验证生成的文件...")
        df_orders_check = pd.read_excel('test_order.xlsx')
        df_schedule_check = pd.read_excel('test_schedule.xlsx')
        
        print(f"\n订单数据验证:")
        print(f"- 总记录数: {len(df_orders_check)}")
        print(f"- 列名: {list(df_orders_check.columns)}")
        print(f"- 数据类型:\n{df_orders_check.dtypes}")
        
        print(f"\n排班表验证:")
        print(f"- 总记录数: {len(df_schedule_check)}")
        print(f"- 列名: {list(df_schedule_check.columns)}")
        print(f"- 数据类型:\n{df_schedule_check.dtypes}")
        
        print("\n测试文件生成完成！")
        
    except Exception as e:
        print(f"生成测试文件时出错: {str(e)}")
        raise

if __name__ == '__main__':
    create_test_files() 