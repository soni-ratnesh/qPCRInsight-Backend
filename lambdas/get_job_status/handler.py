from typing import Any, Dict
import json
import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings
from backend.utils import DecimalEncoder
logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Any:
    """Handle get job status requests.
    
    Args:
        event: API Gateway event
        context: Lambda context
        
    Returns:
        Any: API Gateway response
    """
    try:
        # Extract job_id from path parameters
        job_id = event.get('pathParameters', {}).get('job_id')
        
        if not job_id:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'job_id is required'})
            }
        
        settings = get_settings()
        
        # Get job from DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
        table = dynamodb.Table(settings.JOB_TABLE_NAME)
        
        response = table.get_item(Key={'job_id': job_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'Job not found'})
            }
        
        job = response['Item']
        
        # Prepare response
        result = {
            'job_id': job['job_id'],
            'status': job['status'],
            'created_at': job['created_at'],
            'updated_at': job.get('updated_at'),
            'error_message': job.get('error_message')
        }
        
        # Add result URL if job is complete
        if job['status'] == 'COMPLETED' and 'result_key' in job:
            from backend.services.storage import generate_presigned_get_url
            
            result['result_url'] = generate_presigned_get_url(
                bucket=settings.REPORT_BUCKET_NAME,
                key=job['result_key'],
                expiration=86400  # 24 hours
            )
        
        # Add progress information if available
        if 'progress' in job:
            result['progress'] = job['progress']
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(result, cls=DecimalEncoder)
        }
        
    except Exception as e:
        logger.error(f"Error getting job status: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': 'Internal server error'})
        }