import os
import re
import boto3
import subprocess
from datetime import datetime
from botocore.client import Config
from supabase import create_client, Client
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

# ---------- CONFIG ----------
R2_ACCESS_KEY_ID = "11c972dcf4d706a680d62fc77f0ac94d"
R2_SECRET_ACCESS_KEY = "f3713bf73763d683205106811dd5a65287378d592b2424f2877599b06452e323"
R2_BUCKET_NAME = "chanbox-files"
R2_ENDPOINT_URL = "https://7e868d075fc3878ac28547b3abb90511.r2.cloudflarestorage.com"

SUPABASE_URL = "https://cmmsplgmbrkjylsalrjl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNtbXNwbGdtYnJranlsc2FscmpsIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTM0ODMzOSwiZXhwIjoyMDY0OTI0MzM5fQ.KCF5NpkHUXTXq4xkjf3KFac55UoBQ08txzjan1ovoJM"

# ---------- INIT CLIENTS ----------
s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    config=Config(signature_version="s3v4")
)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------- HELPERS ----------
def sanitize_filename(title: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", title)

def get_file_size(path: str) -> int:
    return os.path.getsize(path) if os.path.exists(path) else 0

# ---------- CORE ----------
def download_and_upload(video_url, user_id=None, folder_id=None, channel_id=None, proxy=None):
    try:
        # Step 1: Get title
        ytdlp_cmd = ["yt-dlp", "--get-title", video_url]
        if proxy:
            ytdlp_cmd.insert(1, f"--proxy={proxy}")
        result = subprocess.run(ytdlp_cmd, capture_output=True, text=True, check=True)
        title = result.stdout.strip()
        safe_title = sanitize_filename(title)
        local_file = f"{safe_title}.mp4"
        object_key = f"uploads/{safe_title}.mp4"

        # Step 2: Download
        dl_cmd = ["yt-dlp", "-f", "best", "-o", local_file, video_url]
        if proxy:
            dl_cmd.insert(1, f"--proxy={proxy}")
        subprocess.run(dl_cmd, check=True)

        # Step 3: Upload multipart to R2
        resp = s3.create_multipart_upload(Bucket=R2_BUCKET_NAME, Key=object_key, ContentType="video/mp4")
        upload_id = resp["UploadId"]
        parts = []

        with open(local_file, "rb") as f:
            part_number = 1
            while True:
                chunk = f.read(20 * 1024 * 1024)
                if not chunk:
                    break
                part_resp = s3.upload_part(
                    Bucket=R2_BUCKET_NAME,
                    Key=object_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )
                etag = part_resp["ETag"].strip('"')
                parts.append({"PartNumber": part_number, "ETag": etag})
                part_number += 1

        s3.complete_multipart_upload(
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )

        # Step 4: Insert Supabase record
        file_size = get_file_size(local_file)
        data = {
            "user_id": user_id,
            "folder_id": folder_id,
            "channel_id": channel_id,
            "file_name": f"{safe_title}.mp4",
            "file_url": object_key,
            "file_type": "video/mp4",
            "file_size": file_size,
            "created_at": datetime.utcnow().isoformat(),
            "is_nsfw": True,
            "is_flagged": False,
            "flagged_reason": None,
            "r2_key": object_key,
            "r2_etag": parts[0]["ETag"] if parts else None,
            "views": 0,
            "phuburl": video_url,
        }
        supabase.table("files").insert(data).execute()

        return {"status": "success", "file_url": object_key}

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        if os.path.exists(loc al_file):
            os.remove(local_file)

# ---------- FASTAPI ----------
app = FastAPI()

class VideoRequest(BaseModel):
    video_url: str
    user_id: str
    folder_id: str = None
    channel_id: str = None
    proxy: str = None

@app.post("/download")
def handle_download(req: VideoRequest):
    return download_and_upload(
        req.video_url,
        user_id=req.user_id,
        folder_id=req.folder_id,
        channel_id=req.channel_id,
        proxy=req.proxy
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
