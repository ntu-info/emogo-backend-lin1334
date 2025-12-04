from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pymongo import MongoClient
from gridfs import GridFS
from bson import ObjectId
import os

app = FastAPI()

# --- Configuration ---
# Replace with your actual MongoDB connection string (e.g., from Render env vars)
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://lin1334:GHz9hDbmMg3y%40CN@fast-api.bwrcbqn.mongodb.net/")
DB_NAME = "media_db"

# --- Database Setup ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
fs = GridFS(db) # Initialize GridFS for file storage

# --- Templates ---
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def list_files(request: Request):
    """
    Renders a page listing all uploaded CSV and Video files.
    """
    # Find all files in GridFS
    files = []
    for grid_out in fs.find():
        files.append({
            "id": str(grid_out._id),
            "filename": grid_out.filename,
            "content_type": grid_out.content_type,
            "upload_date": grid_out.upload_date
        })
    
    return templates.TemplateResponse("index.html", {"request": request, "files": files})

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    """
    Uploads a file to MongoDB GridFS.
    Only allows CSV and Video MIME types.
    """
    # 1. Validate File Type
    allowed_types = ["text/csv", "video/mp4", "video/mpeg", "video/quicktime"]
    
    # Note: Some CSVs might have 'application/vnd.ms-excel' or 'text/plain' depending on OS
    if file.content_type not in allowed_types and not file.filename.endswith(".csv"):
         raise HTTPException(status_code=400, detail="Only CSV and Video files are allowed.")

    # 2. Store in GridFS
    try:
        # fs.put saves the file stream directly into MongoDB chunks
        file_id = fs.put(
            file.file, 
            filename=file.filename, 
            content_type=file.content_type
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    return {"message": "File uploaded successfully", "id": str(file_id)}

@app.get("/download/{file_id}")
async def download_file(file_id: str):
    """
    Streams the file from MongoDB back to the user.
    """
    try:
        # Convert string ID to ObjectId
        oid = ObjectId(file_id)
        
        # Check if file exists
        if not fs.exists(oid):
            raise HTTPException(status_code=404, detail="File not found")
            
        # Open the file from GridFS
        grid_out = fs.get(oid)
        
        # Generator to stream file chunks (memory efficient for large videos)
        def iterfile():
            yield from grid_out
            
        # Set headers for file download
        headers = {
            "Content-Disposition": f'attachment; filename="{grid_out.filename}"'
        }
        
        return StreamingResponse(
            iterfile(), 
            media_type=grid_out.content_type, 
            headers=headers
        )

    except Exception as e:
         raise HTTPException(status_code=404, detail="File not found or invalid ID")