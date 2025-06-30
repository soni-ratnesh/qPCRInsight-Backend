# lambdas/quality_control/handler.py
from typing import Any, Dict
import json
import pandas as pd

import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings
from backend.services.storage import download_json_from_s3, upload_json_to_s3
from backend.qc.replicate import filter_replicates, calculate_replicate_means

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """Perform quality control on qPCR data.
    
    Args:
        event: Contains job_id, parsed_data, and analysis_params
        context: Lambda context
        
    Returns:
        Dict containing QC results
    """
    job_id = event['job_id']
    parsed_data = event['parsed_data']
    analysis_params = event.get('analysis_params', {})
    
    settings = get_settings()
    
    try:
        logger.info(f"Starting quality control for job {job_id}")
        
        # Update job status
        _update_job_status(job_id, 'QUALITY_CONTROL', progress={'step': 'quality_control', 'percentage': 20})
        
        # Load parsed data if it's a key
        if isinstance(parsed_data, str):
            parsed_data = download_json_from_s3(
                bucket=settings.REPORT_BUCKET_NAME,
                key=parsed_data.get('parsed_data_key', parsed_data)
            )
        
        # Convert back to DataFrame
        df = pd.DataFrame(parsed_data['data'])
        
        # Get QC parameters
        sd_cutoff = analysis_params.get('sd_cutoff', 0.5)
        min_proportion = analysis_params.get('min_proportion', 0.5)
        
        # Filter replicates
        filtered_df, flagged_df = filter_replicates(
            df,
            sd_cutoff=sd_cutoff,
            min_proportion=min_proportion,
            ct_column='CT'
        )
        
        # Calculate means for filtered data
        mean_df = calculate_replicate_means(filtered_df)
        
        # Prepare QC summary
        qc_summary = {
            'total_wells': len(df),
            'passed_wells': len(filtered_df),
            'failed_wells': len(flagged_df),
            'pass_rate': len(filtered_df) / len(df) * 100 if len(df) > 0 else 0,
            'n_groups': len(mean_df),
            'qc_parameters': {
                'sd_cutoff': sd_cutoff,
                'min_proportion': min_proportion
            }
        }
        
        # Save QC results to S3
        qc_data_key = f"intermediate/{job_id}/qc_data.json"
        qc_data = {
            'filtered_data': filtered_df.to_dict('records'),
            'flagged_data': flagged_df.to_dict('records') if len(flagged_df) > 0 else [],
            'mean_values': mean_df.to_dict('records'),
            'qc_summary': qc_summary,
            'metadata': parsed_data.get('metadata', {})
        }
        
        upload_json_to_s3(
            data=qc_data,
            bucket=settings.REPORT_BUCKET_NAME,
            key=qc_data_key
        )
        
        # Update job progress
        _update_job_status(job_id, 'QC_COMPLETE', progress={'step': 'quality_control', 'percentage': 100})
        
        logger.info(f"QC complete for job {job_id}: {qc_summary['passed_wells']}/{qc_summary['total_wells']} wells passed")
        
        return {
            'job_id': job_id,
            'qc_data_key': qc_data_key,
            'qc_summary': qc_summary,
            'mean_values_count': len(mean_df)
        }
        
    except Exception as e:
        logger.error(f"QC failed for job {job_id}: {str(e)}")
        _update_job_status(job_id, 'FAILED', error_message=f"QC error: {str(e)}")
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