"""
Study Navigator - Upload Module V2

This module handles uploading compressed files to S3 using presigned URLs for better performance.
"""

import os
import sys
import time
import threading
from typing import Optional, Dict, Any

# Ensure vendor path is loaded
vendor_path = os.path.join(os.path.dirname(__file__), "vendor")
if vendor_path not in sys.path:
    sys.path.append(vendor_path)

import requests
from .session_store import get_valid_token, get_user_auth_info

# API Gateway endpoint for presigned URLs
API_GATEWAY_URL = "https://l9vb3ztz84.execute-api.us-east-2.amazonaws.com/prod"  # API Gateway URL

# Backend API endpoint for progress tracking
BACKEND_API_URL = "http://localhost:8080"  # Backend API URL


# Note: Progress tracking is now handled automatically by Lambda callback
# Lambda calls the backend when processing is complete, eliminating race conditions
# No need for the addon to poll or retry!

def upload_compressed_file(file_path: str, file_type: str = None) -> bool:
    """
    Upload a compressed file to S3 using presigned URLs for better performance.
    Now supports unified export file types with auto-detection.
    
    Args:
        file_path: Path to the compressed file in the exports directory
        file_type: Optional file type override (step1, step2, step3, unified, tag, deck)
    
    Returns:
        bool: Success status
    """
    print(f"[INFO] Starting presigned upload of {file_path}")
    
    # Auto-detect file type from filename if not provided
    if not file_type:
        filename = os.path.basename(file_path)
        if 'all_tags_export' in filename:
            file_type = 'all_tags'
        elif 'unified_export' in filename:
            file_type = 'unified'
        elif 'deck_data' in filename:
            file_type = 'deck'
        else:
            file_type = 'tag'  # Default fallback for existing tag exports
    
    print(f"[INFO] Detected file type: {file_type}")
    
    # Get authentication token
    email, token, synced = get_user_auth_info()
    print(f"[DEBUG] Auth info: email={email is not None}, token={token is not None}, synced={synced}")
    
    if not token:
        print("[ERROR] No valid token available.")
        print("[DEBUG] Please make sure you're logged in through the addon's login dialog.")
        
        # Try to get a fresh token
        try:
            from .session_store import load_session
            session = load_session()
            print(f"[DEBUG] Session file contents: {session}")
        except Exception as e:
            print(f"[DEBUG] Failed to load session: {e}")
        
        return False
    
    # Get file metadata
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    
    print(f"[INFO] Requesting presigned URL for {file_type} file: {file_name} ({file_size} bytes)")
    
    try:
        # Step 1: Request presigned URL
        print(f"[DEBUG] Requesting presigned URL from: {API_GATEWAY_URL}/presigned-upload")
        print(f"[DEBUG] Token length: {len(token) if token else 0}")
        print(f"[DEBUG] Request payload: file_name={file_name}, file_size={file_size}, file_type={file_type}")
        
        presigned_response = requests.post(
            f"{API_GATEWAY_URL}/presigned-upload",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            json={
                "file_name": file_name,
                "file_size": file_size,
                "file_type": file_type
            },
            timeout=10
        )
        
        print(f"[DEBUG] Presigned URL response status: {presigned_response.status_code}")
        print(f"[DEBUG] Presigned URL response headers: {dict(presigned_response.headers)}")
        print(f"[DEBUG] Presigned URL response body: {presigned_response.text}")
        
        if presigned_response.status_code != 200:
            print(f"[ERROR] Failed to get presigned URL: {presigned_response.status_code}")
            print(f"[ERROR] Response: {presigned_response.text}")
            return False
        
        presigned_data = presigned_response.json()
        presigned_url = presigned_data['presigned_url']
        s3_key = presigned_data['s3_key']
        
        print(f"[INFO] Got presigned URL for S3 key: {s3_key}")
        
        # Step 2: Upload directly to S3
        upload_start_time = time.time()
        
        with open(file_path, 'rb') as file:
            # Only use the Content-Type that matches the presigned URL
            upload_response = requests.put(
                presigned_url,
                data=file,
                headers={
                    "Content-Type": "application/gzip"
                },
                timeout=300  # 5 minutes timeout for large files
            )
        
        upload_time_ms = int((time.time() - upload_start_time) * 1000)
        
        if upload_response.status_code == 200:
            print(f"[INFO] Successfully uploaded {file_name} to S3 in {upload_time_ms}ms")
            print(f"[INFO] File will be processed automatically via SQS → Lambda → Supabase")
            
            # Upload successful! Lambda will process asynchronously
            print(f"[INFO] ✅ Upload successful!")
            print(f"[INFO] 🔄 Your data is being processed in the background...")
            
            return True
        else:
            print(f"[ERROR] Failed to upload to S3: {upload_response.status_code}")
            print(f"[ERROR] S3 Response: {upload_response.text}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Exception during presigned upload: {e}")
        return False


def upload_file_in_background(file_path: str, file_type: str = None) -> None:
    """
    Upload a file in a background thread to avoid blocking the UI.
    
    Args:
        file_path: Path to the compressed file
        file_type: Optional file type override (step1, step2, step3, unified, tag, deck)
    """
    if not os.path.exists(file_path):
        print(f"[ERROR] File does not exist: {file_path}")
        return
        
    thread = threading.Thread(target=upload_compressed_file, args=(file_path, file_type))
    thread.daemon = True
    thread.start()
    print(f"[INFO] Started background presigned upload thread for {os.path.basename(file_path)} (type: {file_type})")
