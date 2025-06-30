
from typing import Any, Dict, List, Optional
from datetime import datetime
import uuid
import json

from fastapi import APIRouter, Depends, HTTPException, Header, Query
from pydantic import BaseModel, Field

from backend.auth.jwt import get_current_user
from backend.core.config import get_settings
from backend.services.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/jobs", tags=["jobs"])


class AnalysisParameters(BaseModel):
    """Parameters for qPCR analysis."""
    sd_cutoff: float = Field(default=0.5, description="Standard deviation cutoff for QC")
    min_proportion: float = Field(default=0.5, description="Minimum proportion of valid replicates")
    significance_level: float = Field(default=0.05, description="Statistical significance level")
    p_adjust_method: str = Field(default="fdr_bh", description="P-value adjustment method")
    generate_plots: bool = Field(default=True, description="Whether to generate plots")


class JobSubmitRequest(BaseModel):
    """Request model for job submission."""
    file_key: str = Field(..., description="S3 key of the uploaded file")
    reference_gene: str = Field(..., description="Reference/housekeeping gene name")
    control_condition: str = Field(..., description="Control condition name")
    experiment_name: Optional[str] = Field(None, description="Experiment name")
    analysis_params: AnalysisParameters = Field(default_factory=AnalysisParameters)
    email_notification: bool = Field(default=True, description="Send email when complete")


class JobResponse(BaseModel):
    """Response model for job data."""
    job_id: str = Field(..., description="Unique job identifier")
    status: str = Field(..., description="Job status")
    created_at: str = Field(..., description="Job creation timestamp")
    updated_at: Optional[str] = Field(None, description="Last update timestamp")
    result_url: Optional[str] = Field(None, description="URL to download results")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    progress: Optional[Dict[str, Any]] = Field(None, description="Job progress details")


@router.post("/submit", response_model=JobResponse)
async def submit_job(
    request: JobSubmitRequest,
    authorization: str = Header(None),
    user: Dict[str, Any] = Depends(get_current_user)
) -> JobResponse:
    """Submit analysis job.
    
    Creates a new qPCR analysis job and queues it for processing.
    
    Args:
        request: Job submission request
        authorization: Authorization header
        user: Current user information
        
    Returns:
        JobResponse: Job details
    """
    settings = get_settings()
    user_id = user['user_id']
    
    # Verify file belongs to user
    if not request.file_key.startswith(f"raw/{user_id}/"):
        raise HTTPException(
            status_code=403,
            detail="You don't have permission to analyze this file"
        )
    
    # Generate job ID
    job_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()
    
    try:
        # Create DynamoDB entry
        import boto3
        dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
        table = dynamodb.Table(settings.JOB_TABLE_NAME)
        
        job_item = {
            'job_id': job_id,
            'user_id': user_id,
            'status': 'PENDING',
            'created_at': timestamp,
            'updated_at': timestamp,
            'file_key': request.file_key,
            'reference_gene': request.reference_gene,
            'control_condition': request.control_condition,
            'experiment_name': request.experiment_name or f"Experiment_{timestamp[:10]}",
            'analysis_params': request.analysis_params.dict(),
            'email': user.get('email'),
            'email_notification': request.email_notification
        }
        
        table.put_item(Item=job_item)
        
        # Send message to SQS to trigger processing
        sqs = boto3.client('sqs', region_name=settings.REGION)
        
        message = {
            'job_id': job_id,
            'user_id': user_id,
            'file_key': request.file_key,
            'timestamp': timestamp
        }
        
        # Get queue URL (you might want to add this to settings)
        queue_name = f"{settings.STACK_NAME}-analysis-queue"
        queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
        
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        
        logger.info(f"Created job {job_id} for user {user_id}")
        
        return JobResponse(
            job_id=job_id,
            status='PENDING',
            created_at=timestamp,
            updated_at=timestamp
        )
        
    except Exception as e:
        logger.error(f"Failed to create job: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to create analysis job"
        )


@router.get("/{job_id}/status", response_model=JobResponse)
async def get_job_status(
    job_id: str,
    authorization: str = Header(None),
    user: Dict[str, Any] = Depends(get_current_user)
) -> JobResponse:
    """Get job status.
    
    Args:
        job_id: Job ID
        authorization: Authorization header
        user: Current user information
        
    Returns:
        JobResponse: Job details
    """
    settings = get_settings()
    user_id = user['user_id']
    
    try:
        # Get job from DynamoDB
        import boto3
        dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
        table = dynamodb.Table(settings.JOB_TABLE_NAME)
        
        response = table.get_item(Key={'job_id': job_id})
        
        if 'Item' not in response:
            raise HTTPException(
                status_code=404,
                detail="Job not found"
            )
        
        job = response['Item']
        
        # Verify job belongs to user
        if job['user_id'] != user_id:
            raise HTTPException(
                status_code=403,
                detail="You don't have permission to view this job"
            )
        
        # Prepare response
        job_response = JobResponse(
            job_id=job['job_id'],
            status=job['status'],
            created_at=job['created_at'],
            updated_at=job.get('updated_at'),
            error_message=job.get('error_message')
        )
        
        # Add result URL if job is complete
        if job['status'] == 'COMPLETED' and 'result_key' in job:
            from backend.services.storage import generate_presigned_get_url
            
            job_response.result_url = generate_presigned_get_url(
                bucket=settings.REPORT_BUCKET_NAME,
                key=job['result_key'],
                expiration=86400  # 24 hours
            )
        
        # Add progress information if available
        if 'progress' in job:
            job_response.progress = job['progress']
        
        return job_response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job status: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve job status"
        )


@router.get("/list", response_model=List[JobResponse])
async def list_user_jobs(
    authorization: str = Header(None),
    user: Dict[str, Any] = Depends(get_current_user),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, description="Maximum number of jobs to return"),
    start_date: Optional[str] = Query(None, description="Filter jobs created after this date")
) -> List[JobResponse]:
    """List jobs for the current user.
    
    Args:
        authorization: Authorization header
        user: Current user information
        status: Optional status filter
        limit: Maximum number of jobs to return
        start_date: Optional date filter (ISO format)
        
    Returns:
        List[JobResponse]: List of user's jobs
    """
    settings = get_settings()
    user_id = user['user_id']
    
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr
        
        dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
        table = dynamodb.Table(settings.JOB_TABLE_NAME)
        
        # Query using GSI on user_id
        query_params = {
            'IndexName': 'user-index',
            'KeyConditionExpression': Key('user_id').eq(user_id),
            'Limit': limit,
            'ScanIndexForward': False  # Most recent first
        }
        
        # Add filters if provided
        filter_expressions = []
        
        if status:
            filter_expressions.append(Attr('status').eq(status))
        
        if start_date:
            filter_expressions.append(Attr('created_at').gte(start_date))
        
        if filter_expressions:
            query_params['FilterExpression'] = filter_expressions[0]
            for expr in filter_expressions[1:]:
                query_params['FilterExpression'] = query_params['FilterExpression'] & expr
        
        response = table.query(**query_params)
        
        # Convert to response models
        jobs = []
        for item in response['Items']:
            job_response = JobResponse(
                job_id=item['job_id'],
                status=item['status'],
                created_at=item['created_at'],
                updated_at=item.get('updated_at'),
                error_message=item.get('error_message')
            )
            
            # Add result URL for completed jobs
            if item['status'] == 'COMPLETED' and 'result_key' in item:
                from backend.services.storage import generate_presigned_get_url
                
                job_response.result_url = generate_presigned_get_url(
                    bucket=settings.REPORT_BUCKET_NAME,
                    key=item['result_key'],
                    expiration=86400
                )
            
            jobs.append(job_response)
        
        return jobs
        
    except Exception as e:
        logger.error(f"Failed to list jobs: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve jobs"
        )


@router.get("/{job_id}/download")
async def download_report(
    job_id: str,
    authorization: str = Header(None),
    user: Dict[str, Any] = Depends(get_current_user),
    format: str = Query("zip", description="Download format (zip or individual)")
) -> Dict[str, Any]:
    """Get report download URL.
    
    Args:
        job_id: Job ID
        authorization: Authorization header
        user: Current user information
        format: Download format
        
    Returns:
        Dict[str, Any]: Download URLs
    """
    settings = get_settings()
    user_id = user['user_id']
    
    try:
        # Get job from DynamoDB
        import boto3
        dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
        table = dynamodb.Table(settings.JOB_TABLE_NAME)
        
        response = table.get_item(Key={'job_id': job_id})
        
        if 'Item' not in response:
            raise HTTPException(
                status_code=404,
                detail="Job not found"
            )
        
        job = response['Item']
        
        # Verify job belongs to user
        if job['user_id'] != user_id:
            raise HTTPException(
                status_code=403,
                detail="You don't have permission to download this report"
            )
        
        # Check job is complete
        if job['status'] != 'COMPLETED':
            raise HTTPException(
                status_code=400,
                detail=f"Job is not complete. Current status: {job['status']}"
            )
        
        if 'result_key' not in job:
            raise HTTPException(
                status_code=404,
                detail="No results available for this job"
            )
        
        from backend.services.storage import generate_presigned_get_url
        
        result = {
            'job_id': job_id,
            'download_url': generate_presigned_get_url(
                bucket=settings.REPORT_BUCKET_NAME,
                key=job['result_key'],
                expiration=86400,
                response_content_disposition=f'attachment; filename="qpcr_results_{job_id}.zip"'
            )
        }
        
        # Add individual file URLs if requested and available
        if format == 'individual' and 'result_files' in job:
            result['files'] = {}
            for file_type, file_key in job['result_files'].items():
                result['files'][file_type] = generate_presigned_get_url(
                    bucket=settings.REPORT_BUCKET_NAME,
                    key=file_key,
                    expiration=86400
                )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get download URL: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to generate download URL"
        )


@router.delete("/{job_id}")
async def delete_job(
    job_id: str,
    authorization: str = Header(None),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, str]:
    """Delete a job and its results.
    
    Args:
        job_id: Job ID
        authorization: Authorization header
        user: Current user information
        
    Returns:
        Dict[str, str]: Deletion confirmation
    """
    settings = get_settings()
    user_id = user['user_id']
    
    try:
        import boto3
        dynamodb = boto3.resource('dynamodb', region_name=settings.REGION)
        table = dynamodb.Table(settings.JOB_TABLE_NAME)
        
        # Get job to verify ownership
        response = table.get_item(Key={'job_id': job_id})
        
        if 'Item' not in response:
            raise HTTPException(
                status_code=404,
                detail="Job not found"
            )
        
        job = response['Item']
        
        # Verify job belongs to user
        if job['user_id'] != user_id:
            raise HTTPException(
                status_code=403,
                detail="You don't have permission to delete this job"
            )
        
        # Delete associated files from S3 if they exist
        if 'result_key' in job:
            from backend.services.storage import delete_s3_object
            try:
                delete_s3_object(
                    bucket=settings.REPORT_BUCKET_NAME,
                    key=job['result_key']
                )
            except Exception as e:
                logger.warning(f"Failed to delete result file: {str(e)}")
        
        # Delete job from DynamoDB
        table.delete_item(Key={'job_id': job_id})
        
        logger.info(f"User {user_id} deleted job {job_id}")
        
        return {
            "message": "Job deleted successfully",
            "job_id": job_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete job: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to delete job"
        )