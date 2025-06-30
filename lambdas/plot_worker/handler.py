# lambdas/plot_worker/handler.py
from typing import Any, Dict, List
import json
import os
import tempfile

import boto3
import pandas as pd

from backend.services.logging import get_logger
from backend.core.config import get_settings
from backend.services.storage import download_json_from_s3, upload_bytes_to_s3
from backend.plots.factory import generate_expression_plot, save_plots_to_files

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """Generate plots for qPCR analysis results.
    
    Args:
        event: Contains job_id, analysis_results, and generate_plots flag
        context: Lambda context
        
    Returns:
        Dict containing plot URLs
    """
    job_id = event['job_id']
    analysis_results = event['analysis_results']
    generate_plots = event.get('generate_plots', True)
    
    settings = get_settings()
    
    if not generate_plots:
        logger.info(f"Plot generation disabled for job {job_id}")
        return {
            'job_id': job_id,
            'plot_keys': [],
            'message': 'Plot generation disabled'
        }
    
    try:
        logger.info(f"Starting plot generation for job {job_id}")
        
        # Update job status
        _update_job_status(job_id, 'GENERATING_PLOTS', progress={'step': 'plots', 'percentage': 70})
        
        # Load all analysis data
        fold_change_data = _load_data(analysis_results.get('fold_change_result', {}).get('Payload', {}), 'fold_change_data_key', settings)
        stats_data = _load_data(analysis_results.get('stats_result', {}).get('Payload', {}), 'stats_data_key', settings)
        
        # Convert to DataFrames
        fold_change_df = pd.DataFrame(fold_change_data['fold_change_data'])
        
        # Create plots
        plots = {}
        plot_keys = []
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # 1. Fold Change Bar Plot
            fc_plot = generate_expression_plot(
                fold_change_df,
                x_col='Target Name',
                y_col='fold_change',
                plot_type='bar',
                output_format='png',
                error_col='fold_change_ci_width' if 'fold_change_ci_width' in fold_change_df.columns else None,
                color_col='Sample Name',
                title='Fold Change by Target Gene',
                yaxis_title='Fold Change (2^-ΔΔCt)',
                xaxis_title='Target Gene'
            )
            
            fc_plot_path = os.path.join(temp_dir, 'fold_change_bar.png')
            with open(fc_plot_path, 'wb') as f:
                f.write(fc_plot)
            plots['fold_change_bar'] = fc_plot_path
            
            # 2. Log2 Fold Change Heatmap
            if 'log2_fold_change' in fold_change_df.columns:
                heatmap = generate_expression_plot(
                    fold_change_df,
                    x_col='Sample Name',
                    y_col='Target Name',
                    plot_type='heatmap',
                    output_format='png',
                    values_col='log2_fold_change',
                    title='Log2 Fold Change Heatmap',
                    colorscale='RdBu_r',
                    zmid=0
                )
                
                heatmap_path = os.path.join(temp_dir, 'fold_change_heatmap.png')
                with open(heatmap_path, 'wb') as f:
                    f.write(heatmap)
                plots['fold_change_heatmap'] = heatmap_path
            
            # 3. Delta Ct Distribution
            if 'delta_ct' in fold_change_df.columns:
                delta_ct_plot = generate_expression_plot(
                    fold_change_df,
                    x_col='Target Name',
                    y_col='delta_ct',
                    plot_type='box',
                    output_format='png',
                    color_col='Sample Name',
                    title='ΔCt Distribution by Target',
                    yaxis_title='ΔCt',
                    show_points=True
                )
                
                delta_ct_path = os.path.join(temp_dir, 'delta_ct_distribution.png')
                with open(delta_ct_path, 'wb') as f:
                    f.write(delta_ct_plot)
                plots['delta_ct_distribution'] = delta_ct_path
            
            # 4. Statistical Significance Plot
            if stats_data and 'statistics_results' in stats_data:
                stats_df = pd.DataFrame(stats_data['statistics_results'])
                if len(stats_df) > 0:
                    sig_plot = generate_expression_plot(
                        stats_df,
                        x_col='target',
                        y_col='p_adjusted',
                        plot_type='scatter',
                        output_format='png',
                        title='Statistical Significance by Target',
                        yaxis_title='Adjusted P-value',
                        xaxis_title='Target Gene',
                        marker_size=10
                    )
                    
                    sig_plot_path = os.path.join(temp_dir, 'significance_plot.png')
                    with open(sig_plot_path, 'wb') as f:
                        f.write(sig_plot)
                    plots['significance_plot'] = sig_plot_path
            
            # Upload plots to S3
            for plot_name, plot_path in plots.items():
                plot_key = f"results/{job_id}/plots/{plot_name}.png"
                
                with open(plot_path, 'rb') as f:
                    upload_bytes_to_s3(
                        data=f.read(),
                        bucket=settings.REPORT_BUCKET_NAME,
                        key=plot_key,
                        content_type='image/png'
                    )
                
                plot_keys.append(plot_key)
                logger.info(f"Uploaded plot: {plot_name}")
        
        # Update job progress
        _update_job_status(job_id, 'PLOTS_COMPLETE', progress={'step': 'plots', 'percentage': 100})
        
        logger.info(f"Plot generation complete for job {job_id}")
        
        return {
            'job_id': job_id,
            'plot_keys': plot_keys,
            'plot_count': len(plot_keys)
        }
        
    except Exception as e:
        logger.error(f"Plot generation failed for job {job_id}: {str(e)}")
        _update_job_status(job_id, 'FAILED', error_message=f"Plot generation error: {str(e)}")
        raise


def _load_data(data_ref: Any, key_name: str, settings) -> Dict[str, Any]:
    """Load data from S3 if it's a reference."""
    if isinstance(data_ref, str):
        return download_json_from_s3(
            bucket=settings.REPORT_BUCKET_NAME,
            key=data_ref
        )
    elif isinstance(data_ref, dict) and key_name in data_ref:
        return download_json_from_s3(
            bucket=settings.REPORT_BUCKET_NAME,
            key=data_ref[key_name]
        )
    return data_ref


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