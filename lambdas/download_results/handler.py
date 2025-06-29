from typing import Any, Dict
import json
import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings
from backend.services.storage import generate_presigned_get_url

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Any:
    """Handle download results requests.
    
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
        
        # Check job is complete
        if job['status'] != 'COMPLETED':
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': f'Job is not complete. Current status: {job["status"]}'
                })
            }
        
        if 'result_key' not in job:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'No results available for this job'})
            }
        
        # Generate presigned URL for download
        download_url = generate_presigned_get_url(
            bucket=settings.REPORT_BUCKET_NAME,
            key=job['result_key'],
            expiration=86400,  # 24 hours
            response_content_disposition=f'attachment; filename="qpcr_results_{job_id}.zip"'
        )
        
        result = {
            'job_id': job_id,
            'download_url': download_url,
            'filename': f"qpcr_results_{job_id}.zip",
            'expires_in': 86400
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(result)
        }
        
    except Exception as e:
        logger.error(f"Error generating download URL: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': 'Internal server error'})
        }