# lambdas/normalize_data/handler.py
from typing import Any, Dict
import json
import pandas as pd

import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings
from backend.services.storage import download_json_from_s3, upload_json_to_s3
from backend.analysis.normalize import compute_delta_ct, compute_delta_delta_ct

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """Normalize qPCR data (ΔCt and ΔΔCt calculations).
    
    Args:
        event: Contains job_id, qc_data, reference_gene, control_condition
        context: Lambda context
        
    Returns:
        Dict containing normalized data
    """
    job_id = event['job_id']
    qc_data = event['qc_data']
    reference_gene = event.get('reference_gene', 'GAPDH')
    control_condition = event.get('control_condition', 'CONTROL')
    
    settings = get_settings()
    
    try:
        logger.info(f"Starting normalization for job {job_id}")
        
        # Update job status
        _update_job_status(job_id, 'NORMALIZING', progress={'step': 'normalization', 'percentage': 40})
        
        # Load QC data if it's a key
        if isinstance(qc_data, dict):
            qc_data = download_json_from_s3(
                bucket=settings.REPORT_BUCKET_NAME,
                key=qc_data.get('qc_data_key', qc_data)
            )
        
        # Convert mean values to DataFrame
        mean_df = pd.DataFrame(qc_data['mean_values'])
        
        # Compute ΔCt
        delta_ct_df = compute_delta_ct(
            mean_df,
            reference_gene=reference_gene,
            target_genes=None,  # All non-reference genes
            ct_column='ct_mean',
            sample_column='Sample Name',
            target_column='Target Name'
        )
        
        # Compute ΔΔCt
        delta_delta_ct_df = compute_delta_delta_ct(
            delta_ct_df,
            control_condition=control_condition,
            experimental_conditions=None,  # All non-control conditions
            delta_ct_column='delta_ct',
            sample_column='Sample Name',
            target_column='Target Name'
        )
        
        # Prepare normalization summary
        normalization_summary = {
            'reference_gene': reference_gene,
            'control_condition': control_condition,
            'n_samples': delta_delta_ct_df['Sample Name'].nunique(),
            'n_targets': delta_delta_ct_df['Target Name'].nunique(),
            'delta_ct_range': {
                'min': float(delta_ct_df['delta_ct'].min()),
                'max': float(delta_ct_df['delta_ct'].max())
            },
            'delta_delta_ct_range': {
                'min': float(delta_delta_ct_df['delta_delta_ct'].min()),
                'max': float(delta_delta_ct_df['delta_delta_ct'].max())
            }
        }
        
        # Save normalized data to S3
        normalized_data_key = f"intermediate/{job_id}/normalized_data.json"
        normalized_data = {
            'delta_ct_data': delta_ct_df.to_dict('records'),
            'delta_delta_ct_data': delta_delta_ct_df.to_dict('records'),
            'normalization_summary': normalization_summary,
            'metadata': qc_data.get('metadata', {})
        }
        
        upload_json_to_s3(
            data=normalized_data,
            bucket=settings.REPORT_BUCKET_NAME,
            key=normalized_data_key
        )
        
        # Update job progress
        _update_job_status(job_id, 'NORMALIZATION_COMPLETE', progress={'step': 'normalization', 'percentage': 100})
        
        logger.info(f"Normalization complete for job {job_id}")
        
        return {
            'job_id': job_id,
            'normalized_data_key': normalized_data_key,
            'normalization_summary': normalization_summary
        }
        
    except Exception as e:
        logger.error(f"Normalization failed for job {job_id}: {str(e)}")
        _update_job_status(job_id, 'FAILED', error_message=f"Normalization error: {str(e)}")
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