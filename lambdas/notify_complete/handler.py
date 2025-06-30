# lambdas/notify_complete/handler.py
from typing import Any, Dict
import json
import os

import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """Handle job completion and send notifications.
    
    Args:
        event: Contains job_id, report_result, email, and notification settings
        context: Lambda context
        
    Returns:
        Dict containing completion status
    """
    job_id = event['job_id']
    report_result = event.get('report_result', {})
    email = event.get('email')
    email_notification = event.get('email_notification', False)
    
    settings = get_settings()
    
    try:
        logger.info(f"Completing job {job_id}")
        
        # Update job status to COMPLETED
        _update_job_status(job_id, 'COMPLETED', progress={'step': 'complete', 'percentage': 100})
        
        # Send email notification if requested
        if email_notification and email:
            _send_completion_email(job_id, email, report_result, settings)
        
        logger.info(f"Job {job_id} completed successfully")
        
        return {
            'job_id': job_id,
            'status': 'COMPLETED',
            'notification_sent': email_notification and email is not None,
            'report_url': report_result.get('package_result', {}).get('zip_url')
        }
        
    except Exception as e:
        logger.error(f"Failed to complete job {job_id}: {str(e)}")
        _update_job_status(job_id, 'FAILED', error_message=f"Completion error: {str(e)}")
        raise


def _send_completion_email(job_id: str, email: str, report_result: Dict[str, Any], settings):
    """Send completion email using SES."""
    ses = boto3.client('ses', region_name=settings.REGION)
    
    # Get download URL
    download_url = report_result.get('package_result', {}).get('zip_url', '')
    
    # Email content
    subject = f"qPCR Analysis Complete - Job {job_id[:8]}"
    
    body_html = f"""
    <html>
        <body>
            <h2>Your qPCR Analysis is Complete!</h2>
            <p>Job ID: <strong>{job_id}</strong></p>
            
            <p>Your analysis has been completed successfully. You can download the results using the link below:</p>
            
            <p><a href="{download_url}" style="background-color: #4CAF50; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Download Results</a></p>
            
            <p><em>Note: This download link will expire in 24 hours.</em></p>
            
            <h3>Analysis Summary:</h3>
            <ul>
                <li>Report Types: {', '.join(report_result.get('report_types', ['Excel', 'PDF']))}</li>
                <li>Files Included: {report_result.get('package_result', {}).get('file_count', 'N/A')}</li>
            </ul>
            
            <p>Best regards,<br>qPCR Analysis Platform</p>
        </body>
    </html>
    """
    
    body_text = f"""
Your qPCR Analysis is Complete!

Job ID: {job_id}

Your analysis has been completed successfully. You can download the results here:
{download_url}

Note: This download link will expire in 24 hours.

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
        logger.info(f"Sent completion email to {email} for job {job_id}")
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        # Don't fail the job if email fails


def _update_job_status(job_id: str, status: str, progress: Dict[str, Any] = None, error_message: str = None):
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
    
    if progress:
        update_expr += ", progress = :progress"
        expr_values[':progress'] = progress
    
    if error_message:
        update_expr += ", error_message = :error"
        expr_values[':error'] = error_message
    
    # Add completion timestamp if status is COMPLETED
    if status == 'COMPLETED':
        update_expr += ", completed_at = :completed"
        expr_values[':completed'] = datetime.now().isoformat()
    
    table.update_item(
        Key={'job_id': job_id},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames=expr_names
    )