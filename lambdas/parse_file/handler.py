# lambdas/parse_file/handler.py
from typing import Any, Dict
import json
import tempfile
import os

import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings
from backend.services.storage import download_bytes_from_s3, upload_json_to_s3
from backend.ingest.parser import parse_applied_biosystems_xlsx, validate_data_quality

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """Parse qPCR Excel file and extract data.
    
    Args:
        event: Contains job_id and file_key
        context: Lambda context
        
    Returns:
        Dict containing parsed data and metadata
    """
    job_id = event['job_id']
    file_key = event['file_key']
    
    settings = get_settings()
    
    try:
        logger.info(f"Starting file parse for job {job_id}")
        
        # Update job status
        _update_job_status(job_id, 'PARSING', progress={'step': 'parse_file', 'percentage': 10})
        
        # Download file from S3
        file_data = download_bytes_from_s3(
            bucket=settings.RAW_BUCKET_NAME,
            key=file_key
        )
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            tmp_file.write(file_data)
            tmp_file.flush()
            
            # Parse the file
            df, metadata = parse_applied_biosystems_xlsx(tmp_file.name)
            
            # Validate data quality
            df = validate_data_quality(df)
            
            # Clean up
            os.unlink(tmp_file.name)
        
        # Save parsed data to S3 for next step
        parsed_data_key = f"intermediate/{job_id}/parsed_data.json"
        
        # Convert DataFrame to dict for JSON serialization
        parsed_data = {
            'data': df.to_dict('records'),
            'metadata': metadata,
            'columns': list(df.columns),
            'shape': list(df.shape),
            'summary': {
                'n_samples': df['Sample Name'].nunique(),
                'n_targets': df['Target Name'].nunique(),
                'n_wells': len(df),
                'has_undetermined': df['ct_undetermined'].any()
            }
        }
        
        upload_json_to_s3(
            data=parsed_data,
            bucket=settings.REPORT_BUCKET_NAME,
            key=parsed_data_key
        )
        
        # Update job progress
        _update_job_status(job_id, 'PARSING_COMPLETE', progress={'step': 'parse_file', 'percentage': 100})
        
        logger.info(f"Successfully parsed file for job {job_id}")
        
        return {
            'job_id': job_id,
            'parsed_data_key': parsed_data_key,
            'summary': parsed_data['summary'],
            'metadata': metadata
        }
        
    except Exception as e:
        logger.error(f"Failed to parse file for job {job_id}: {str(e)}")
        _update_job_status(job_id, 'FAILED', error_message=f"Parse error: {str(e)}")
        raise


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
    
    table.update_item(
        Key={'job_id': job_id},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames=expr_names
    )