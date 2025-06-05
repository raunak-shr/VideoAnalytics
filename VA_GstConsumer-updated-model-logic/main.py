# incomplete, needs to be concrete
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
import subprocess

app = FastAPI()
script_process = None

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
async def redirect():
    return RedirectResponse("/docs")

@app.post("/start-script")
async def start_script():
    global script_process
    if script_process is None:
        script_process = subprocess.Popen(['python', 'consumers/congestion_consumer.py'])
        return JSONResponse(content={"status": "Script started"}, status_code=200)
    else:
        return JSONResponse(content={"status": "Script is already running"}, status_code=400)


@app.post("/stop-script")
async def stop_script():
    global script_process
    if script_process is not None:
        script_process.terminate()
        script_process = None
        return JSONResponse(content={"status": "Script stopped"}, status_code=200)
    else:
        return JSONResponse(content={"status": "Script is not running"}, status_code=400)


# if __name__ == '__main__':
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000, reload=True, )
