
from typing import Any, Dict, Optional
import json
from datetime import datetime
import mimetypes

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from backend.core.config import get_settings
from backend.services.logging import get_logger

logger = get_logger(__name__)


def _get_s3_client():
    """Get S3 client with lazy loading.
    
    Returns:
        boto3.client: S3 client
    """
    settings = get_settings()
    return boto3.client('s3', region_name=settings.REGION)


def generate_presigned_put_url(
    bucket: str,
    key: str,
    expiration: int = 3600,
    content_type: Optional[str] = None,
    metadata: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Generate presigned URL for S3 PUT.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        expiration: URL expiration in seconds
        content_type: MIME type of the content
        metadata: Optional object metadata
        
    Returns:
        Dict[str, Any]: Presigned URL and fields
            {
                "url": "https://...",
                "fields": {
                    "key": "...",
                    "AWSAccessKeyId": "...",
                    "policy": "...",
                    "signature": "..."
                }
            }
            
    Raises:
        ClientError: If S3 operation fails
    """
    s3_client = _get_s3_client()
    
    try:
        # Prepare conditions
        conditions = [
            ["content-length-range", 0, 104857600],  # Max 100MB
        ]
        
        fields = {}
        
        if content_type:
            conditions.append(["eq", "$Content-Type", content_type])
            fields["Content-Type"] = content_type
        
        if metadata:
            for k, v in metadata.items():
                meta_key = f"x-amz-meta-{k}"
                conditions.append(["eq", f"${meta_key}", v])
                fields[meta_key] = v
        
        # Generate presigned POST
        response = s3_client.generate_presigned_post(
            Bucket=bucket,
            Key=key,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=expiration
        )
        
        logger.info(f"Generated presigned PUT URL for s3://{bucket}/{key}")
        return response
        
    except ClientError as e:
        logger.error(f"Failed to generate presigned PUT URL: {str(e)}")
        raise


def generate_presigned_get_url(
    bucket: str,
    key: str,
    expiration: int = 3600,
    response_content_disposition: Optional[str] = None
) -> str:
    """Generate presigned URL for S3 GET.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        expiration: URL expiration in seconds
        response_content_disposition: Content-Disposition header value
        
    Returns:
        str: Presigned URL
        
    Raises:
        ClientError: If S3 operation fails
    """
    s3_client = _get_s3_client()
    
    try:
        params = {
            'Bucket': bucket,
            'Key': key
        }
        
        if response_content_disposition:
            params['ResponseContentDisposition'] = response_content_disposition
        
        url = s3_client.generate_presigned_url(
            'get_object',
            Params=params,
            ExpiresIn=expiration
        )
        
        logger.info(f"Generated presigned GET URL for s3://{bucket}/{key}")
        return url
        
    except ClientError as e:
        logger.error(f"Failed to generate presigned GET URL: {str(e)}")
        raise


def upload_bytes_to_s3(
    data: bytes,
    bucket: str,
    key: str,
    metadata: Optional[Dict[str, str]] = None,
    content_type: Optional[str] = None
) -> bool:
    """Upload bytes to S3.
    
    Args:
        data: Bytes data to upload
        bucket: S3 bucket name
        key: S3 object key
        metadata: Optional object metadata
        content_type: MIME type of the content
        
    Returns:
        bool: Success status
        
    Raises:
        ClientError: If S3 operation fails
    """
    s3_client = _get_s3_client()
    
    try:
        # Prepare put arguments
        put_args = {
            'Bucket': bucket,
            'Key': key,
            'Body': data
        }
        
        # Add content type if provided or detect it
        if content_type:
            put_args['ContentType'] = content_type
        else:
            # Try to detect from key
            content_type, _ = mimetypes.guess_type(key)
            if content_type:
                put_args['ContentType'] = content_type
        
        # Add metadata
        if metadata:
            put_args['Metadata'] = metadata
        
        # Upload
        s3_client.put_object(**put_args)
        
        logger.info(f"Successfully uploaded {len(data)} bytes to s3://{bucket}/{key}")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during S3 upload: {str(e)}")
        raise


def download_bytes_from_s3(
    bucket: str,
    key: str
) -> bytes:
    """Download bytes from S3.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        bytes: Downloaded data
        
    Raises:
        ClientError: If S3 operation fails
    """
    s3_client = _get_s3_client()
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        
        logger.info(f"Successfully downloaded {len(data)} bytes from s3://{bucket}/{key}")
        return data
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"Object not found: s3://{bucket}/{key}")
        else:
            logger.error(f"Failed to download from S3: {str(e)}")
        raise


def list_s3_objects(
    bucket: str,
    prefix: str = "",
    max_keys: int = 1000
) -> List[Dict[str, Any]]:
    """List objects in S3 bucket.
    
    Args:
        bucket: S3 bucket name
        prefix: Key prefix to filter by
        max_keys: Maximum number of keys to return
        
    Returns:
        List[Dict[str, Any]]: List of object metadata
    """
    s3_client = _get_s3_client()
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=max_keys
        )
        
        objects = []
        if 'Contents' in response:
            for obj in response['Contents']:
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'etag': obj['ETag'].strip('"')
                })
        
        return objects
        
    except ClientError as e:
        logger.error(f"Failed to list S3 objects: {str(e)}")
        raise


def delete_s3_object(
    bucket: str,
    key: str
) -> bool:
    """Delete object from S3.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        bool: Success status
    """
    s3_client = _get_s3_client()
    
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Successfully deleted s3://{bucket}/{key}")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to delete S3 object: {str(e)}")
        raise


def upload_json_to_s3(
    data: Dict[str, Any],
    bucket: str,
    key: str,
    metadata: Optional[Dict[str, str]] = None
) -> bool:
    """Upload JSON data to S3.
    
    Args:
        data: Dictionary to serialize as JSON
        bucket: S3 bucket name
        key: S3 object key
        metadata: Optional object metadata
        
    Returns:
        bool: Success status
    """
    try:
        json_bytes = json.dumps(data, indent=2, default=str).encode('utf-8')
        return upload_bytes_to_s3(
            data=json_bytes,
            bucket=bucket,
            key=key,
            metadata=metadata,
            content_type='application/json'
        )
    except Exception as e:
        logger.error(f"Failed to upload JSON to S3: {str(e)}")
        raise


def download_json_from_s3(
    bucket: str,
    key: str
) -> Dict[str, Any]:
    """Download and parse JSON from S3.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        Dict[str, Any]: Parsed JSON data
    """
    try:
        data = download_bytes_from_s3(bucket, key)
        return json.loads(data.decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from S3: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to download JSON from S3: {str(e)}")
        raise