# lambdas/fold_change/handler.py
from typing import Any, Dict
import json
import pandas as pd

import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings
from backend.services.storage import download_json_from_s3, upload_json_to_s3
from backend.analysis.fold_change import compute_fold_change, summarize_fold_changes

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """Calculate fold changes from normalized data.
    
    Args:
        event: Contains job_id and normalized_data
        context: Lambda context
        
    Returns:
        Dict containing fold change results
    """
    job_id = event['job_id']
    normalized_data = event['normalized_data']
    
    settings = get_settings()
    
    try:
        logger.info(f"Starting fold change calculation for job {job_id}")
        
        # Update job status
        _update_job_status(job_id, 'CALCULATING_FOLD_CHANGE', progress={'step': 'fold_change', 'percentage': 50})
        
        # Load normalized data if it's a key
        if isinstance(normalized_data, dict):
            normalized_data = download_json_from_s3(
                bucket=settings.REPORT_BUCKET_NAME,
                key=normalized_data.get('normalized_data_key', normalized_data)
            )
        
        # Convert to DataFrame
        delta_delta_ct_df = pd.DataFrame(normalized_data['delta_delta_ct_data'])
        
        # Compute fold change
        fold_change_df = compute_fold_change(
            delta_delta_ct_df,
            delta_delta_ct_col='delta_delta_ct',
            log2_transform=True,
            add_confidence_interval=True,
            ct_std_col='ct_std' if 'ct_std' in delta_delta_ct_df.columns else None
        )
        
        # Summarize fold changes
        fold_change_summary = summarize_fold_changes(
            fold_change_df,
            group_by=['Target Name'],
            fold_change_col='fold_change'
        )
        
        # Prepare analysis summary
        analysis_summary = {
            'n_upregulated': (fold_change_df['regulation'] == 'Upregulated').sum(),
            'n_downregulated': (fold_change_df['regulation'] == 'Downregulated').sum(),
            'n_no_change': (fold_change_df['regulation'] == 'No change').sum(),
            'n_undetermined': (fold_change_df['regulation'] == 'Undetermined').sum(),
            'fold_change_range': {
                'min': float(fold_change_df['fold_change'].min()),
                'max': float(fold_change_df['fold_change'].max())
            }
        }
        
        # Save fold change data to S3
        fold_change_data_key = f"intermediate/{job_id}/fold_change_data.json"
        fold_change_data = {
            'fold_change_data': fold_change_df.to_dict('records'),
            'fold_change_summary': fold_change_summary.to_dict('records'),
            'analysis_summary': analysis_summary,
            'metadata': normalized_data.get('metadata', {})
        }
        
        upload_json_to_s3(
            data=fold_change_data,
            bucket=settings.REPORT_BUCKET_NAME,
            key=fold_change_data_key
        )
        
        # Update job progress
        _update_job_status(job_id, 'FOLD_CHANGE_COMPLETE', progress={'step': 'fold_change', 'percentage': 100})
        
        logger.info(f"Fold change calculation complete for job {job_id}")
        
        return {
            'job_id': job_id,
            'fold_change_data_key': fold_change_data_key,
            'analysis_summary': analysis_summary
        }
        
    except Exception as e:
        logger.error(f"Fold change calculation failed for job {job_id}: {str(e)}")
        _update_job_status(job_id, 'FAILED', error_message=f"Fold change error: {str(e)}")
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