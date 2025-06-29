
from typing import Dict, List, Optional
import os
import zipfile
import tempfile
from datetime import datetime
import io

from backend.services.storage import (
    upload_bytes_to_s3, 
    generate_presigned_get_url,
    upload_json_to_s3
)
from backend.services.logging import get_logger

logger = get_logger(__name__)


def package_and_upload(
    file_paths: List[str],
    job_id: str,
    bucket_name: str,
    metadata: Optional[Dict[str, str]] = None,
    create_manifest: bool = True
) -> Dict[str, str]:
    """Package files into zip and upload to S3.
    
    Creates a zip archive containing all specified files and uploads it to S3.
    Optionally creates a manifest file listing all included files.
    
    Args:
        file_paths: List of file paths to package
        job_id: Job ID for S3 key prefix
        bucket_name: S3 bucket name
        metadata: Optional metadata to attach to S3 object
        create_manifest: Whether to create a manifest file
        
    Returns:
        Dict[str, str]: Dictionary with:
            - zip_key: S3 key of the uploaded zip file
            - zip_url: Presigned URL for downloading the zip
            - manifest_key: S3 key of the manifest (if created)
            - file_count: Number of files packaged
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_key = f"reports/{job_id}/qpcr_results_{timestamp}.zip"
    
    # Create zip archive in memory
    zip_buffer = io.BytesIO()
    
    manifest = {
        'job_id': job_id,
        'created_at': datetime.now().isoformat(),
        'files': []
    }
    
    try:
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_path in file_paths:
                if os.path.exists(file_path):
                    # Get just the filename for the archive
                    arcname = os.path.basename(file_path)
                    
                    # Add file to zip
                    zip_file.write(file_path, arcname)
                    
                    # Add to manifest
                    file_info = {
                        'filename': arcname,
                        'original_path': file_path,
                        'size': os.path.getsize(file_path),
                        'modified': datetime.fromtimestamp(
                            os.path.getmtime(file_path)
                        ).isoformat()
                    }
                    manifest['files'].append(file_info)
                    
                    logger.info(f"Added {arcname} to zip archive")
                else:
                    logger.warning(f"File not found: {file_path}")
        
        # Get zip data
        zip_data = zip_buffer.getvalue()
        
        # Upload zip to S3
        zip_metadata = metadata or {}
        zip_metadata.update({
            'job-id': job_id,
            'file-count': str(len(manifest['files'])),
            'package-type': 'qpcr-results'
        })
        
        upload_success = upload_bytes_to_s3(
            data=zip_data,
            bucket=bucket_name,
            key=zip_key,
            metadata=zip_metadata,
            content_type='application/zip'
        )
        
        if not upload_success:
            raise Exception("Failed to upload zip to S3")
        
        # Upload manifest if requested
        manifest_key = None
        if create_manifest:
            manifest_key = f"reports/{job_id}/manifest_{timestamp}.json"
            upload_json_to_s3(
                data=manifest,
                bucket=bucket_name,
                key=manifest_key,
                metadata={'job-id': job_id}
            )
        
        # Generate presigned URL for download
        zip_url = generate_presigned_get_url(
            bucket=bucket_name,
            key=zip_key,
            expiration=86400,  # 24 hours
            response_content_disposition=f'attachment; filename="qpcr_results_{job_id}.zip"'
        )
        
        result = {
            'zip_key': zip_key,
            'zip_url': zip_url,
            'file_count': len(manifest['files']),
            'zip_size': len(zip_data)
        }
        
        if manifest_key:
            result['manifest_key'] = manifest_key
        
        logger.info(f"Successfully packaged and uploaded {len(manifest['files'])} files for job {job_id}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to package and upload files: {str(e)}")
        raise


def create_report_package(
    analysis_results: Dict[str, Any],
    job_id: str,
    bucket_name: str,
    include_raw_data: bool = False
) -> Dict[str, str]:
    """Create a complete report package from analysis results.
    
    Args:
        analysis_results: Complete analysis results dictionary
        job_id: Job ID
        bucket_name: S3 bucket name
        include_raw_data: Whether to include raw data files
        
    Returns:
        Dict[str, str]: Package information including download URL
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        files_to_package = []
        
        # Save reports if they exist
        if 'report_paths' in analysis_results:
            for report_type, path in analysis_results['report_paths'].items():
                if os.path.exists(path):
                    files_to_package.append(path)
        
        # Save plots if they exist
        if 'plot_paths' in analysis_results:
            for plot_path in analysis_results['plot_paths']:
                if os.path.exists(plot_path):
                    files_to_package.append(plot_path)
        
        # Save summary JSON
        summary_path = os.path.join(temp_dir, 'analysis_summary.json')
        summary_data = {
            'job_id': job_id,
            'completed_at': datetime.now().isoformat(),
            'parameters': analysis_results.get('parameters', {}),
            'statistics': analysis_results.get('statistics_summary', {}),
            'qc_summary': analysis_results.get('qc_summary', {})
        }
        
        import json
        with open(summary_path, 'w') as f:
            json.dump(summary_data, f, indent=2)
        files_to_package.append(summary_path)
        
        # Include raw data if requested
        if include_raw_data and 'raw_data_path' in analysis_results:
            raw_path = analysis_results['raw_data_path']
            if os.path.exists(raw_path):
                files_to_package.append(raw_path)
        
        # Package and upload
        return package_and_upload(
            file_paths=files_to_package,
            job_id=job_id,
            bucket_name=bucket_name,
            metadata={
                'analysis-type': 'qpcr-delta-delta-ct',
                'report-version': '1.0'
            }
        )


def package_error_report(
    job_id: str,
    error_message: str,
    error_details: Dict[str, Any],
    bucket_name: str
) -> Dict[str, str]:
    """Package error information for failed jobs.
    
    Args:
        job_id: Job ID
        error_message: Error message
        error_details: Detailed error information
        bucket_name: S3 bucket name
        
    Returns:
        Dict[str, str]: Error report information
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_key = f"errors/{job_id}/error_report_{timestamp}.json"
    
    error_data = {
        'job_id': job_id,
        'error_time': datetime.now().isoformat(),
        'error_message': error_message,
        'error_details': error_details
    }
    
    # Upload error report
    upload_json_to_s3(
        data=error_data,
        bucket=bucket_name,
        key=error_key,
        metadata={'job-id': job_id, 'status': 'error'}
    )
    
    # Generate presigned URL
    error_url = generate_presigned_get_url(
        bucket=bucket_name,
        key=error_key,
        expiration=86400  # 24 hours
    )
    
    return {
        'error_key': error_key,
        'error_url': error_url
    }