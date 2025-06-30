#!/bin/bash

# Configuration
STACK_NAME=${STACK_NAME:-"qpcr-analysis"}
REGION=${AWS_REGION:-"us-east-1"}

echo -e "Cleaning and deploying qPCR Analysis Platform$"

# Step 1: Delete the existing stack if it exists
echo -e "Checking for existing stack...$"
if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" &>/dev/null; then
    echo -e "Deleting existing stack...$"
    aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"
    echo "Waiting for stack deletion..."
    aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"
    echo -e "Stack deleted$"
fi

# Step 2: Clean build artifacts
echo -e "Cleaning build artifacts..."
rm -rf .aws-sam
rm -rf infra/.aws-sam
rm -f samconfig.toml

# Step 3: Create S3 bucket for deployment
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
S3_BUCKET="sam-deployments-${ACCOUNT_ID}-${REGION}"

echo -e "${YELLOW}Creating deployment bucket: $S3_BUCKET${NC}"
aws s3 mb "s3://$S3_BUCKET" --region "$REGION" 2>/dev/null || true

# Step 4: Package the template manually
cd ./infra

sam build

sam deploy --guided