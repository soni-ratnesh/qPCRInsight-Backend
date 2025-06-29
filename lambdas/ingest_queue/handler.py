from typing import Any, Dict
import json
import boto3
import os

from backend.services.logging import get_logger
from backend.core.config import get_settings

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Any:
    """Handle S3 PUT events for raw files and queue them for processing.
    
    Args:
        event: S3 event
        context: Lambda context
        
    Returns:
        Any: Processing result
    """
    settings = get_settings()
    sqs = boto3.client('sqs', region_name=settings.REGION)
    
    # Get SQS queue URL from environment
    queue_url = os.environ.get('ANALYSIS_QUEUE_URL')
    if not queue_url:
        logger.error("ANALYSIS_QUEUE_URL environment variable not set")
        return {'statusCode': 500, 'body': 'Configuration error'}
    
    processed_count = 0
    
    for record in event.get('Records', []):
        try:
            # Extract S3 information
            s3_info = record['s3']
            bucket = s3_info['bucket']['name']
            key = s3_info['object']['key']
            
            logger.info(f"Processing S3 event for bucket: {bucket}, key: {key}")
            
            # Only process files in the raw/ prefix
            if not key.startswith('raw/'):
                logger.info(f"Skipping non-raw file: {key}")
                continue
            
            # Get file metadata from S3
            s3 = boto3.client('s3', region_name=settings.REGION)
            
            try:
                head_response = s3.head_object(Bucket=bucket, Key=key)
                metadata = head_response.get('Metadata', {})
            except Exception as e:
                logger.error(f"Failed to get object metadata: {str(e)}")
                metadata = {}
            
            # Create SQS message
            message = {
                'action': 'process_file',
                'bucket': bucket,
                'key': key,
                'metadata': metadata,
                'event_time': record.get('eventTime'),
                's3_event': s3_info
            }
            
            # Send to SQS
            sqs_response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message),
                MessageAttributes={
                    'file_key': {
                        'StringValue': key,
                        'DataType': 'String'
                    }
                }
            )
            
            logger.info(f"Queued file for processing: {key}, MessageId: {sqs_response['MessageId']}")
            processed_count += 1
            
        except Exception as e:
            logger.error(f"Error processing S3 event: {str(e)}", exc_info=True)
            # Continue processing other records
            continue
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Processed {processed_count} files',
            'processed_count': processed_count
        })
    }