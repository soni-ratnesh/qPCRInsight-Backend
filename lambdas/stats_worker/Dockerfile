# lambdas/stats_worker/Dockerfile
FROM public.ecr.aws/lambda/python:3.11

# Copy requirements and install dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

# Copy the backend package
COPY ../../backend ${LAMBDA_TASK_ROOT}/backend

# Copy the lambda function
COPY handler.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD [ "handler.lambda_handler" ]