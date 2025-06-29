
from typing import Any, Dict, Optional
from datetime import datetime
import uuid

from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel, Field

from backend.auth.jwt import get_current_user
from backend.services.storage import generate_presigned_put_url
from backend.core.config import get_settings
from backend.services.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/files", tags=["files"])


class PresignRequest(BaseModel):
    """Request model for presigned URL generation."""
    filename: str = Field(..., description="Name of the file to upload")
    content_type: str = Field(
        default="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        description="MIME type of the file"
    )
    file_size: Optional[int] = Field(None, description="Size of file in bytes")


class PresignResponse(BaseModel):
    """Response model for presigned URL."""
    upload_url: str = Field(..., description="URL to POST the file to")
    upload_fields: Dict[str, str] = Field(..., description="Fields to include in the POST request")
    key: str = Field(..., description="S3 key where file will be stored")
    expires_at: str = Field(..., description="When the presigned URL expires")


@router.post("/presign", response_model=PresignResponse)
async def create_presigned_upload(
    request: PresignRequest,
    authorization: str = Header(None),
    user: Dict[str, Any] = Depends(get_current_user)
) -> PresignResponse:
    """Create presigned URL for file upload.
    
    Generates a presigned POST URL that allows the client to upload
    a file directly to S3 without going through the Lambda function.
    
    Args:
        request: Presign request data
        authorization: Authorization header (automatically captured)
        user: Current user information from JWT
        
    Returns:
        PresignResponse: Presigned URL and upload fields
        
    Raises:
        HTTPException: If presigned URL generation fails
    """
    settings = get_settings()
    
    # Validate file type
    allowed_types = [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
        "text/csv"
    ]
    
    if request.content_type not in allowed_types:
        raise HTTPException(
            status_code=400,
            detail=f"File type not allowed. Allowed types: {', '.join(allowed_types)}"
        )
    
    # Validate file size if provided
    max_size = 100 * 1024 * 1024  # 100 MB
    if request.file_size and request.file_size > max_size:
        raise HTTPException(
            status_code=400,
            detail=f"File too large. Maximum size: {max_size} bytes"
        )
    
    try:
        # Generate unique key for the file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_id = str(uuid.uuid4())[:8]
        user_id = user['user_id']
        
        # Clean filename
        import os
        safe_filename = os.path.basename(request.filename).replace(" ", "_")
        
        # Create S3 key
        key = f"raw/{user_id}/{timestamp}_{file_id}_{safe_filename}"
        
        # Generate presigned URL
        expires_in = 3600  # 1 hour
        
        metadata = {
            'user-id': user_id,
            'original-filename': request.filename,
            'upload-timestamp': timestamp
        }
        
        response = generate_presigned_put_url(
            bucket=settings.RAW_BUCKET_NAME,
            key=key,
            expiration=expires_in,
            content_type=request.content_type,
            metadata=metadata
        )
        
        # Calculate expiration time
        from datetime import timedelta
        expires_at = (datetime.now() + timedelta(seconds=expires_in)).isoformat()
        
        logger.info(f"Generated presigned URL for user {user_id}, file: {request.filename}")
        
        return PresignResponse(
            upload_url=response['url'],
            upload_fields=response['fields'],
            key=key,
            expires_at=expires_at
        )
        
    except Exception as e:
        logger.error(f"Failed to generate presigned URL: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to generate upload URL"
        )


class FileListResponse(BaseModel):
    """Response model for file listing."""
    files: List[Dict[str, Any]] = Field(..., description="List of user's files")
    total_count: int = Field(..., description="Total number of files")


@router.get("/list", response_model=FileListResponse)
async def list_user_files(
    authorization: str = Header(None),
    user: Dict[str, Any] = Depends(get_current_user),
    limit: int = 100,
    prefix: Optional[str] = None
) -> FileListResponse:
    """List files uploaded by the current user.
    
    Args:
        authorization: Authorization header
        user: Current user information
        limit: Maximum number of files to return
        prefix: Optional prefix to filter files
        
    Returns:
        FileListResponse: List of user's files
    """
    from backend.services.storage import list_s3_objects
    
    settings = get_settings()
    user_id = user['user_id']
    
    # Build prefix
    search_prefix = f"raw/{user_id}/"
    if prefix:
        search_prefix += prefix
    
    try:
        objects = list_s3_objects(
            bucket=settings.RAW_BUCKET_NAME,
            prefix=search_prefix,
            max_keys=limit
        )
        
        # Format response
        files = []
        for obj in objects:
            # Extract filename from key
            filename = obj['key'].split('/')[-1]
            # Remove timestamp and file_id prefix
            parts = filename.split('_', 2)
            if len(parts) >= 3:
                display_name = parts[2]
            else:
                display_name = filename
            
            files.append({
                'key': obj['key'],
                'filename': display_name,
                'size': obj['size'],
                'last_modified': obj['last_modified'],
                'etag': obj['etag']
            })
        
        return FileListResponse(
            files=files,
            total_count=len(files)
        )
        
    except Exception as e:
        logger.error(f"Failed to list files: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to list files"
        )


@router.delete("/{file_key:path}")
async def delete_file(
    file_key: str,
    authorization: str = Header(None),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, str]:
    """Delete a file uploaded by the user.
    
    Args:
        file_key: S3 key of the file to delete
        authorization: Authorization header
        user: Current user information
        
    Returns:
        Dict[str, str]: Deletion confirmation
    """
    from backend.services.storage import delete_s3_object
    
    settings = get_settings()
    user_id = user['user_id']
    
    # Verify file belongs to user
    if not file_key.startswith(f"raw/{user_id}/"):
        raise HTTPException(
            status_code=403,
            detail="You don't have permission to delete this file"
        )
    
    try:
        delete_s3_object(
            bucket=settings.RAW_BUCKET_NAME,
            key=file_key
        )
        
        logger.info(f"User {user_id} deleted file: {file_key}")
        
        return {
            "message": "File deleted successfully",
            "key": file_key
        }
        
    except Exception as e:
        logger.error(f"Failed to delete file: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to delete file"
        )