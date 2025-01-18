import pytest
from fastapi.testclient import TestClient
from api.process import app
import pandas as pd
from io import BytesIO
import uuid
import asyncio

# 创建测试客户端
client = TestClient(app)

@pytest.fixture(scope="session")
def event_loop():
    """创建一个事件循环，供整个测试会话使用"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def test_excel_files():
    """创建测试用的Excel文件"""
    # 创建订单数据
    order_data = pd.DataFrame({
        '主订单编号': ['1', '2', '3'],
        '子订单编号': ['1-1', '2-1', '3-1'],
        '商品ID': ['001', '002', '003'],
        '选购商品': ['测试商品1', '测试商品2', '测试商品3'],
        '流量来源': ['直播', '直播', '直播'],
        '流量体裁': ['直播', '直播', '直播'],
        '取消原因': [None, None, None],
        '订单状态': ['已完成', '已完成', '已完成'],
        '订单应付金额': [100, 200, 300],
        '订单提交日期': ['2024-01-01', '2024-01-01', '2024-01-01'],
        '订单提交时间': ['12:00:00', '13:00:00', '14:00:00']
    })
    
    # 创建排班表数据
    schedule_data = pd.DataFrame({
        '日期': ['2024-01-01', '2024-01-01', '2024-01-01'],
        '上播时间': ['11:00:00', '12:00:00', '13:00:00'],
        '下播时间': ['12:00:00', '13:00:00', '14:00:00'],
        '主播姓名': ['主播A', '主播B', '主播C'],
        '场控姓名': ['场控A', '场控B', '场控C'],
        '时段消耗': [50, 60, 70]
    })
    
    # 将数据保存到BytesIO对象
    order_buffer = BytesIO()
    schedule_buffer = BytesIO()
    
    order_data.to_excel(order_buffer, index=False)
    schedule_data.to_excel(schedule_buffer, index=False)
    
    order_buffer.seek(0)
    schedule_buffer.seek(0)
    
    return order_buffer, schedule_buffer

@pytest.fixture
def task_id(test_excel_files):
    """创建一个任务并返回任务ID"""
    order_file, schedule_file = test_excel_files
    
    files = {
        'order_file': ('order.xlsx', order_file, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
        'schedule_file': ('schedule.xlsx', schedule_file, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    }
    
    response = client.post("/api/process", files=files)
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    return data["task_id"]

def test_upload_and_process(task_id):
    """测试文件上传和处理"""
    assert task_id is not None
    assert isinstance(task_id, str)

def test_task_status_not_found():
    """测试不存在的任务ID"""
    response = client.get(f"/api/status/{uuid.uuid4()}")
    assert response.status_code == 404
    assert "error" in response.json()

def test_task_status_processing(task_id):
    """测试处理中的任务状态"""
    response = client.get(f"/api/status/{task_id}")
    
    # 检查响应类型
    content_type = response.headers.get("content-type", "")
    
    if "application/json" in content_type:
        data = response.json()
        assert response.status_code == 200
        assert "status" in data
        assert "progress" in data
        assert "message" in data
    elif "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in content_type:
        assert response.status_code == 200
        assert len(response.content) > 0
    else:
        pytest.fail(f"Unexpected content type: {content_type}")

def test_task_status_completed(task_id):
    """测试已完成的任务状态"""
    # 等待任务完成
    import time
    max_wait = 30  # 最多等待30秒
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        response = client.get(f"/api/status/{task_id}")
        content_type = response.headers.get("content-type", "")
        
        if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in content_type:
            # 任务完成，返回Excel文件
            assert response.status_code == 200
            assert len(response.content) > 0
            break
        elif "application/json" in content_type:
            data = response.json()
            if data.get("status") == "completed":
                # 任务完成，但没有结果
                assert data["progress"] == 100
                break
            elif data.get("status") == "failed":
                pytest.fail(f"Task failed: {data.get('message')}")
        
        time.sleep(1)
    else:
        pytest.fail("Task did not complete within the timeout period") 