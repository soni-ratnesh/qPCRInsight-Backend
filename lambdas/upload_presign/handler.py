from typing import Any, Dict
import json
import os
from datetime import datetime, timedelta
import uuid

from backend.services.logging import get_logger
from backend.services.storage import generate_presigned_put_url
from backend.core.config import get_settings

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Any:
    """Handle presigned URL generation requests from API Gateway.
    
    Args:
        event: API Gateway event
        context: Lambda context
        
    Returns:
        Any: API Gateway response
    """
    logger.info("Received presigned URL request")
    
    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        
        # Extract parameters
        filename = body.get('filename')
        content_type = body.get('content_type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        
        if not filename:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'Filename is required'})
            }
        
        settings = get_settings()
        
        # Generate unique key
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_id = str(uuid.uuid4())[:8]
        safe_filename = os.path.basename(filename).replace(" ", "_")
        
        # Create S3 key without user_id
        key = f"raw/{timestamp}_{file_id}_{safe_filename}"
        
        # Generate presigned URL
        metadata = {
            'original-filename': filename,
            'upload-timestamp': timestamp
        }
        
        response = generate_presigned_put_url(
            bucket=settings.RAW_BUCKET_NAME,
            key=key,
            expiration=3600,
            content_type=content_type,
            metadata=metadata
        )
        
        # Calculate expiration
        expires_at = (datetime.now() + timedelta(seconds=3600)).isoformat()
        
        result = {
            'upload_url': response['url'],
            'upload_fields': response['fields'],
            'key': key,
            'expires_at': expires_at
        }
        
        logger.info(f"Generated presigned URL for file: {filename}")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(result)
        }
        
    except Exception as e:
        logger.error(f"Error generating presigned URL: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': 'Internal server error'})
        }