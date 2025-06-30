# lambdas/error_handler/handler.py
from typing import Any, Dict
import json

import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings
from backend.report.packager import package_error_report

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """Handle errors in the analysis pipeline.
    
    Args:
        event: Contains job_id and error information
        context: Lambda context
        
    Returns:
        Dict containing error handling result
    """
    job_id = event['job_id']
    error_info = event.get('error', {})
    
    settings = get_settings()
    
    try:
        logger.error(f"Handling error for job {job_id}: {json.dumps(error_info)}")
        
        # Extract error details
        error_message = "Analysis failed"
        error_details = {
            'error_type': 'Unknown',
            'error_message': 'Unknown error occurred',
            'stack_trace': None,
            'step': 'Unknown'
        }
        
        if isinstance(error_info, dict):
            if 'Cause' in error_info:
                # Step Functions error format
                try:
                    cause = json.loads(error_info['Cause'])
                    error_message = cause.get('errorMessage', 'Analysis failed')
                    error_details['error_type'] = cause.get('errorType', 'Unknown')
                    error_details['error_message'] = error_message
                    error_details['stack_trace'] = cause.get('stackTrace', [])
                except:
                    error_message = str(error_info.get('Cause', 'Analysis failed'))
            else:
                error_message = error_info.get('error_message', 'Analysis failed')
                error_details.update(error_info)
        
        # Determine which step failed
        if 'progress' in event:
            error_details['step'] = event['progress'].get('step', 'Unknown')
        
        # Package error report
        error_report = package_error_report(
            job_id=job_id,
            error_message=error_message,
            error_details=error_details,
            bucket_name=settings.REPORT_BUCKET_NAME
        )
        
        # Update job status
        _update_job_status(
            job_id=job_id,
            status='FAILED',
            error_message=error_message,
            error_details=error_details
        )
        
        # Send error notification if email is available
        job_details = _get_job_details(job_id)
        if job_details.get('email_notification') and job_details.get('email'):
            _send_error_email(
                job_id=job_id,
                email=job_details['email'],
                error_message=error_message,
                error_details=error_details
            )
        
        logger.info(f"Error handling complete for job {job_id}")
        
        return {
            'job_id': job_id,
            'status': 'FAILED',
            'error_report': error_report,
            'error_message': error_message
        }
        
    except Exception as e:
        logger.error(f"Failed to handle error for job {job_id}: {str(e)}")
        # Try to at least update the job status
        try:
            _update_job_status(job_id, 'FAILED', error_message=f"Error handler failed: {str(e)}")
        except:
            pass
        raise


def _update_job_status(job_id: str, status: str, error_message: str = None, error_details: Dict[str, Any] = None):
    """Update job status in DynamoDB."""
    settings = get_settings()
    dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
    table = dynamodb.Table(settings.JOB_TABLE_NAME)
    
    from datetime import datetime
    
    update_expr = "SET #status = :status, updated_at = :timestamp"
    expr_values = {
        ':status': status,
        ':timestamp': datetime.now().isoformat()
    }
    expr_names = {'#status': 'status'}
    
    if error_message:
        update_expr += ", error_message = :error"
        expr_values[':error'] = error_message
    
    if error_details:
        update_expr += ", error_details = :details"
        expr_values[':details'] = error_details
    
    # Add failure timestamp
    update_expr += ", failed_at = :failed"
    expr_values[':failed'] = datetime.now().isoformat()
    
    table.update_item(
        Key={'job_id': job_id},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames=expr_names
    )


def _get_job_details(job_id: str) -> Dict[str, Any]:
    """Get job details from DynamoDB."""
    settings = get_settings()
    dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
    table = dynamodb.Table(settings.JOB_TABLE_NAME)
    
    response = table.get_item(Key={'job_id': job_id})
    return response.get('Item', {})


def _send_error_email(job_id: str, email: str, error_message: str, error_details: Dict[str, Any]):
    """Send error notification email."""
    settings = get_settings()
    ses = boto3.client('ses', region_name=settings.REGION)
    
    subject = f"qPCR Analysis Failed - Job {job_id[:8]}"
    
    body_html = f"""
    <html>
        <body>
            <h2>qPCR Analysis Failed</h2>
            <p>Job ID: <strong>{job_id}</strong></p>
            
            <p>Unfortunately, your analysis encountered an error:</p>
            
            <div style="background-color: #f8d7da; color: #721c24; padding: 10px; border-radius: 5px; margin: 10px 0;">
                <strong>Error:</strong> {error_message}
            </div>
            
            <h3>Error Details:</h3>
            <ul>
                <li><strong>Step:</strong> {error_details.get('step', 'Unknown')}</li>
                <li><strong>Type:</strong> {error_details.get('error_type', 'Unknown')}</li>
            </ul>
            
            <p>Please check your input file and parameters, then try again. If the problem persists, please contact support.</p>
            
            <p>Best regards,<br>qPCR Analysis Platform</p>
        </body>
    </html>
    """
    
    body_text = f"""
qPCR Analysis Failed

Job ID: {job_id}

Unfortunately, your analysis encountered an error:
{error_message}

Error Details:
- Step: {error_details.get('step', 'Unknown')}
- Type: {error_details.get('error_type', 'Unknown')}

Please check your input file and parameters, then try again. If the problem persists, please contact support.

Best regards,
qPCR Analysis Platform
    """
    
    try:
        ses.send_email(
            Source=os.environ.get('SES_FROM_EMAIL', 'noreply@qpcr-analysis.com'),
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {
                    'Text': {'Data': body_text},
                    'Html': {'Data': body_html}
                }
            }
        )
        logger.info(f"Sent error email to {email} for job {job_id}")
    except Exception as e:
        logger.error(f"Failed to send error email: {str(e)}")