# lambdas/stats_worker/handler.py
from typing import Any, Dict
import json
import pandas as pd

import boto3

from backend.services.logging import get_logger
from backend.core.config import get_settings
from backend.services.storage import download_json_from_s3, upload_json_to_s3
from backend.stats.tests import run_stat_tests
from backend.stats.posthoc import run_pairwise_tests, adjust_pvalues

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """Perform statistical analysis on qPCR data.
    
    Args:
        event: Contains job_id, analysis_data, and analysis_params
        context: Lambda context
        
    Returns:
        Dict containing statistical results
    """
    job_id = event['job_id']
    analysis_data = event['analysis_data']
    analysis_params = event.get('analysis_params', {})
    
    settings = get_settings()
    
    try:
        logger.info(f"Starting statistical analysis for job {job_id}")
        
        # Update job status
        _update_job_status(job_id, 'STATISTICAL_ANALYSIS', progress={'step': 'statistics', 'percentage': 60})
        
        # Load fold change data
        fold_change_data = analysis_data.get('fold_change_result', {}).get('Payload', {})
        if isinstance(fold_change_data, str):
            fold_change_data = download_json_from_s3(
                bucket=settings.REPORT_BUCKET_NAME,
                key=fold_change_data.get('fold_change_data_key', fold_change_data)
            )
        
        # Convert to DataFrame
        df = pd.DataFrame(fold_change_data['fold_change_data'])
        
        # Get statistical parameters
        significance_level = analysis_params.get('significance_level', 0.05)
        p_adjust_method = analysis_params.get('p_adjust_method', 'fdr_bh')
        
        # Run statistical tests for each target
        stats_results = []
        targets = df['Target Name'].unique()
        
        for target in targets:
            target_df = df[df['Target Name'] == target]
            
            if target_df['Sample Name'].nunique() >= 2:
                # Run appropriate test
                test_result = run_stat_tests(
                    target_df,
                    groups='Sample Name',
                    values_col='fold_change',
                    test_type='auto',
                    alpha=significance_level
                )
                
                test_result['target'] = target
                stats_results.append(test_result)
                
                # If significant and more than 2 groups, run post-hoc
                if test_result['significant'] and test_result['n_groups'] > 2:
                    posthoc_result = run_pairwise_tests(
                        target_df,
                        group_col='Sample Name',
                        value_col='fold_change',
                        test_type='tukey' if test_result['assumptions'].get('normality', False) else 'dunn',
                        p_adjust=p_adjust_method
                    )
                    test_result['posthoc'] = posthoc_result.to_dict('records')
        
        # Adjust p-values across all tests
        if stats_results:
            all_pvalues = [r['p_value'] for r in stats_results]
            adjusted_pvalues = adjust_pvalues(all_pvalues, method=p_adjust_method, alpha=significance_level)
            
            for i, result in enumerate(stats_results):
                result['p_adjusted'] = float(adjusted_pvalues[i])
                result['significant_adjusted'] = result['p_adjusted'] < significance_level
        
        # Prepare statistics summary
        stats_summary = {
            'n_tests': len(stats_results),
            'n_significant': sum(1 for r in stats_results if r.get('significant_adjusted', False)),
            'significance_level': significance_level,
            'adjustment_method': p_adjust_method,
            'test_types_used': list(set(r['test_name'] for r in stats_results))
        }
        
        # Save statistics results to S3
        stats_data_key = f"intermediate/{job_id}/stats_data.json"
        stats_data = {
            'statistics_results': stats_results,
            'statistics_summary': stats_summary,
            'metadata': fold_change_data.get('metadata', {})
        }
        
        upload_json_to_s3(
            data=stats_data,
            bucket=settings.REPORT_BUCKET_NAME,
            key=stats_data_key
        )
        
        # Update job progress
        _update_job_status(job_id, 'STATISTICS_COMPLETE', progress={'step': 'statistics', 'percentage': 100})
        
        logger.info(f"Statistical analysis complete for job {job_id}")
        
        return {
            'job_id': job_id,
            'stats_data_key': stats_data_key,
            'statistics_summary': stats_summary
        }
        
    except Exception as e:
        logger.error(f"Statistical analysis failed for job {job_id}: {str(e)}")
        _update_job_status(job_id, 'FAILED', error_message=f"Statistics error: {str(e)}")
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