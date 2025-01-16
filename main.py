import uvicorn
from api.process import app

if __name__ == "__main__":
    uvicorn.run(
        "api.process:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 