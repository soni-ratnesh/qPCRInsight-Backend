# lambdas/analysis_runner/handler.py
from typing import Any, Dict, Optional
import json
import os

import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Any:
    """Handle SQS messages and trigger Step Functions execution.
    
    This Lambda function is triggered by SQS messages and starts
    the Step Functions state machine for qPCR analysis.
    
    Args:
        event: SQS event containing job information
        context: Lambda context
        
    Returns:
        Any: Processing result
    """
    settings = get_settings()
    
    # Process each record (usually just one)
    for record in event.get('Records', []):
        try:
            # Parse message
            message = json.loads(record['body'])
            
            # Check if this is from S3 event or job submission
            if 'job_id' in message:
                # Direct job submission
                job_id = message['job_id']
                file_key = message['file_key']
            else:
                # From S3 event - need to check if there's a pending job
                file_key = message['key']
                job_id = _find_or_create_job(file_key)
            
            logger.info(f"Processing job {job_id} for file {file_key}")
            
            # Update job status to PROCESSING
            _update_job_status(job_id, 'PROCESSING')
            
            # Get job details from DynamoDB
            job_details = _get_job_details(job_id)
            
            # Start Step Functions execution
            sfn_client = boto3.client('stepfunctions', region_name=settings.REGION)
            
            # Prepare execution input
            execution_input = {
                'job_id': job_id,
                'file_key': file_key,
                'reference_gene': job_details.get('reference_gene', 'GAPDH'),
                'control_condition': job_details.get('control_condition', 'CONTROL'),
                'experiment_name': job_details.get('experiment_name', f'Experiment_{job_id[:8]}'),
                'analysis_params': job_details.get('analysis_params', {}),
                'email': job_details.get('email'),
                'email_notification': job_details.get('email_notification', False)
            }
            
            # Get state machine ARN from environment or construct it
            state_machine_arn = os.environ.get('STATE_MACHINE_ARN')
            if not state_machine_arn:
                # Build ARN from environment variables
                stack_name = os.environ.get('STACK_NAME')
                if not stack_name:
                    raise ValueError("STACK_NAME environment variable not set")
                
                account_id = boto3.client('sts').get_caller_identity()['Account']
                region = settings.REGION
                state_machine_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{stack_name}-analysis-workflow"
            
            # Start execution
            try:
                execution_response = sfn_client.start_execution(
                    stateMachineArn=state_machine_arn,
                    name=f"job-{job_id}-{int(context.aws_request_id[-8:], 16)}",  # Unique name
                    input=json.dumps(execution_input)
                )
                
                # Update job with execution ARN
                _update_job_field(job_id, 'execution_arn', execution_response['executionArn'])
                
                logger.info(f"Started Step Functions execution for job {job_id}: {execution_response['executionArn']}")
                
            except sfn_client.exceptions.ExecutionAlreadyExists:
                logger.warning(f"Execution already exists for job {job_id}")
                # Don't fail - the job is already being processed
                
        except Exception as e:
            logger.error(f"Failed to process job: {str(e)}")
            if 'job_id' in locals():
                _update_job_status(job_id, 'FAILED', error_message=str(e))
            
            # Don't re-raise to avoid reprocessing
            continue
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Processing completed'})
    }


def _find_or_create_job(file_key: str) -> str:
    """Find existing job for file or create new one.
    
    Args:
        file_key: S3 file key
        
    Returns:
        str: Job ID
    """
    settings = get_settings()
    dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
    table = dynamodb.Table(settings.JOB_TABLE_NAME)
    
    # For now, create a new job
    # In a real system, you might want to check if a job already exists
    import uuid
    from datetime import datetime
    
    job_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()
    
    # Extract user_id from file key if present (format: raw/user_id/...)
    user_id = 'anonymous'
    if file_key.startswith('raw/'):
        parts = file_key.split('/')
        if len(parts) >= 3:
            user_id = parts[1]
    
    job_item = {
        'job_id': job_id,
        'user_id': user_id,
        'status': 'PENDING',
        'created_at': timestamp,
        'updated_at': timestamp,
        'file_key': file_key,
        'reference_gene': 'GAPDH',  # Default values
        'control_condition': 'CONTROL',
        'experiment_name': f'Auto_{timestamp[:10]}',
        'analysis_params': {
            'sd_cutoff': 0.5,
            'min_proportion': 0.5,
            'significance_level': 0.05,
            'p_adjust_method': 'fdr_bh',
            'generate_plots': True
        }
    }
    
    table.put_item(Item=job_item)
    
    return job_id


def _get_job_details(job_id: str) -> Dict[str, Any]:
    """Get job details from DynamoDB.
    
    Args:
        job_id: Job ID
        
    Returns:
        Dict[str, Any]: Job details
    """
    settings = get_settings()
    dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
    table = dynamodb.Table(settings.JOB_TABLE_NAME)
    
    response = table.get_item(Key={'job_id': job_id})
    
    if 'Item' not in response:
        raise ValueError(f"Job {job_id} not found")
    
    return response['Item']


def _update_job_status(
    job_id: str, 
    status: str, 
    error_message: Optional[str] = None
) -> None:
    """Update job status in DynamoDB.
    
    Args:
        job_id: Job ID
        status: New status
        error_message: Optional error message
    """
    settings = get_settings()
    dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
    table = dynamodb.Table(settings.JOB_TABLE_NAME)
    
    from datetime import datetime
    
    update_expr = "SET #status = :status, updated_at = :timestamp"
    expr_values = {
        ':status': status,
        ':timestamp': datetime.now().isoformat()
    }
    expr_names = {
        '#status': 'status'
    }
    
    if error_message:
        update_expr += ", error_message = :error"
        expr_values[':error'] = error_message
    
    table.update_item(
        Key={'job_id': job_id},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames=expr_names
    )


def _update_job_field(job_id: str, field: str, value: Any) -> None:
    """Update a specific field in the job record.
    
    Args:
        job_id: Job ID
        field: Field name to update
        value: New value
    """
    settings = get_settings()
    dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
    table = dynamodb.Table(settings.JOB_TABLE_NAME)
    
    table.update_item(
        Key={'job_id': job_id},
        UpdateExpression=f"SET {field} = :value",
        ExpressionAttributeValues={':value': value}
    )