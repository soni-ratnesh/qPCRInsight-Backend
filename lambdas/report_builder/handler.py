# lambdas/report_builder/handler.py
from typing import Any, Dict
import json
import os

import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings
from backend.services.storage import download_json_from_s3
from backend.report.builder import create_analysis_report
from backend.report.packager import package_and_upload

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """Build comprehensive analysis report.
    
    Args:
        event: Contains job_id, experiment_name, and all_results
        context: Lambda context
        
    Returns:
        Dict containing report package information
    """
    job_id = event['job_id']
    experiment_name = event.get('experiment_name', f'qPCR_Analysis_{job_id[:8]}')
    all_results = event['all_results']
    
    settings = get_settings()
    
    try:
        logger.info(f"Starting report building for job {job_id}")
        
        # Update job status
        _update_job_status(job_id, 'BUILDING_REPORT', progress={'step': 'report', 'percentage': 80})
        
        # Gather all analysis results
        analysis_results = {}
        
        # Load data from each step
        if 'parse_result' in all_results:
            parsed_data = _load_result_data(all_results['parse_result'], 'parsed_data_key', settings)
            analysis_results['raw_data'] = pd.DataFrame(parsed_data['data'])
            analysis_results['metadata'] = parsed_data.get('metadata', {})
        
        if 'qc_result' in all_results:
            qc_data = _load_result_data(all_results['qc_result'], 'qc_data_key', settings)
            analysis_results['mean_values'] = pd.DataFrame(qc_data['mean_values'])
            analysis_results['qc_summary'] = pd.DataFrame([qc_data['qc_summary']])
        
        if 'normalize_result' in all_results:
            norm_data = _load_result_data(all_results['normalize_result'], 'normalized_data_key', settings)
            analysis_results['delta_ct'] = pd.DataFrame(norm_data['delta_ct_data'])
            analysis_results['delta_delta_ct'] = pd.DataFrame(norm_data['delta_delta_ct_data'])
        
        if 'fold_change_result' in all_results:
            fc_data = _load_result_data(all_results['fold_change_result'], 'fold_change_data_key', settings)
            analysis_results['fold_change_data'] = pd.DataFrame(fc_data['fold_change_data'])
            analysis_results['fold_change_summary'] = pd.DataFrame(fc_data['fold_change_summary'])
        
        if 'stats_result' in all_results:
            stats_data = _load_result_data(all_results['stats_result'], 'stats_data_key', settings)
            if stats_data.get('statistics_results'):
                analysis_results['statistics'] = pd.DataFrame(stats_data['statistics_results'])
            analysis_results['statistics_summary'] = stats_data.get('statistics_summary', {})
        
        # Get plot paths if available
        if 'plot_result' in all_results:
            plot_result = all_results['plot_result'].get('Payload', {})
            analysis_results['plot_paths'] = [
                f"s3://{settings.REPORT_BUCKET_NAME}/{key}" 
                for key in plot_result.get('plot_keys', [])
            ]
        
        # Add analysis parameters
        analysis_results['parameters'] = all_results.get('analysis_params', {})
        
        # Create reports in temporary directory
        import tempfile
        import pandas as pd
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Generate Excel and PDF reports
            report_paths = create_analysis_report(
                analysis_results=analysis_results,
                experiment_name=experiment_name,
                output_dir=temp_dir
            )
            
            # Package and upload reports
            file_paths = list(report_paths.values())
            
            # Add plots if they exist locally
            if 'plot_result' in all_results:
                plot_keys = all_results['plot_result'].get('Payload', {}).get('plot_keys', [])
                # Download plots temporarily for packaging
                for plot_key in plot_keys:
                    plot_data = download_bytes_from_s3(
                        bucket=settings.REPORT_BUCKET_NAME,
                        key=plot_key
                    )
                    plot_path = os.path.join(temp_dir, os.path.basename(plot_key))
                    with open(plot_path, 'wb') as f:
                        f.write(plot_data)
                    file_paths.append(plot_path)
            
            # Package all files
            package_result = package_and_upload(
                file_paths=file_paths,
                job_id=job_id,
                bucket_name=settings.REPORT_BUCKET_NAME,
                metadata={
                    'experiment-name': experiment_name,
                    'analysis-type': 'qpcr-delta-delta-ct'
                }
            )
        
        # Update job with result information
        _update_job_field(job_id, 'result_key', package_result['zip_key'])
        _update_job_field(job_id, 'result_files', report_paths)
        
        # Update job progress
        _update_job_status(job_id, 'REPORT_COMPLETE', progress={'step': 'report', 'percentage': 100})
        
        logger.info(f"Report building complete for job {job_id}")
        
        return {
            'job_id': job_id,
            'package_result': package_result,
            'report_types': list(report_paths.keys())
        }
        
    except Exception as e:
        logger.error(f"Report building failed for job {job_id}: {str(e)}")
        _update_job_status(job_id, 'FAILED', error_message=f"Report building error: {str(e)}")
        raise


def _load_result_data(result: Dict[str, Any], key_name: str, settings) -> Dict[str, Any]:
    """Load result data from S3."""
    payload = result.get('Payload', {})
    if isinstance(payload, dict) and key_name in payload:
        return download_json_from_s3(
            bucket=settings.REPORT_BUCKET_NAME,
            key=payload[key_name]
        )
    return payload


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


def _update_job_field(job_id: str, field: str, value: Any):
    """Update a specific field in the job record."""
    settings = get_settings()
    dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
    table = dynamodb.Table(settings.JOB_TABLE_NAME)
    
    table.update_item(
        Key={'job_id': job_id},
        UpdateExpression=f"SET {field} = :value",
        ExpressionAttributeValues={':value': value}
    )