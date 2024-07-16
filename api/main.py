from fastapi import FastAPI
from api.routes.complaints import router as complaints_router

app = FastAPI()

# Include the complaints router
app.include_router(complaints_router, prefix="/api")

# Start the FastAPI server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
