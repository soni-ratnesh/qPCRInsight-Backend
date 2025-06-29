from typing import Any, Dict
import json
import boto3
from datetime import datetime
import uuid

from backend.services.logging import get_logger
from backend.core.config import get_settings

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Any:
    """Handle job submission requests.
    
    Args:
        event: API Gateway event
        context: Lambda context
        
    Returns:
        Any: API Gateway response
    """
    logger.info("Received job submission request")
    
    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        
        # Extract parameters
        file_key = body.get('file_key')
        reference_gene = body.get('reference_gene', 'GAPDH')
        control_condition = body.get('control_condition', 'CONTROL')
        experiment_name = body.get('experiment_name')
        analysis_params = body.get('analysis_params', {})
        email = body.get('email')  # Optional email for notifications
        
        if not file_key:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'file_key is required'})
            }
        
        settings = get_settings()
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat()
        
        # Create DynamoDB entry
        dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
        table = dynamodb.Table(settings.JOB_TABLE_NAME)
        
        job_item = {
            'job_id': job_id,
            'status': 'PENDING',
            'created_at': timestamp,
            'updated_at': timestamp,
            'file_key': file_key,
            'reference_gene': reference_gene,
            'control_condition': control_condition,
            'experiment_name': experiment_name or f"Experiment_{timestamp[:10]}",
            'analysis_params': analysis_params,
            'email': email,
            'email_notification': bool(email)
        }
        
        table.put_item(Item=job_item)
        
        # Send message to SQS to trigger processing
        sqs = boto3.client('sqs', region_name=settings.REGION)
        
        # Get queue URL from environment
        queue_url = os.environ.get('ANALYSIS_QUEUE_URL')
        if not queue_url:
            # Try to get it from queue name
            queue_name = f"{settings.STACK_NAME}-analysis-queue"
            try:
                queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
            except:
                logger.error(f"Could not find queue: {queue_name}")
                raise
        
        message = {
            'job_id': job_id,
            'file_key': file_key,
            'timestamp': timestamp
        }
        
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        
        logger.info(f"Created job {job_id}")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'job_id': job_id,
                'status': 'PENDING',
                'created_at': timestamp
            })
        }
        
    except Exception as e:
        logger.error(f"Error submitting job: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': 'Internal server error'})
        }