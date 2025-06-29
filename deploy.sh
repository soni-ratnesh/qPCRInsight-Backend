#!/bin/bash
set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Configuration
STACK_NAME=${STACK_NAME:-"qpcr-analysis"}
REGION=${AWS_REGION:-"us-east-1"}

echo -e "${GREEN}🧹 Cleaning and deploying qPCR Analysis Platform${NC}"

# Step 1: Delete the existing stack if it exists
echo -e "${YELLOW}Checking for existing stack...${NC}"
if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" &>/dev/null; then
    echo -e "${YELLOW}Deleting existing stack...${NC}"
    aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"
    echo "Waiting for stack deletion..."
    aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"
    echo -e "${GREEN}✓ Stack deleted${NC}"
fi

# Step 2: Clean build artifacts
echo -e "${YELLOW}Cleaning build artifacts...${NC}"
rm -rf .aws-sam
rm -rf infra/.aws-sam
rm -f samconfig.toml

# Step 3: Create S3 bucket for deployment
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
S3_BUCKET="sam-deployments-${ACCOUNT_ID}-${REGION}"

echo -e "${YELLOW}Creating deployment bucket: $S3_BUCKET${NC}"
aws s3 mb "s3://$S3_BUCKET" --region "$REGION" 2>/dev/null || true

# Step 4: Package the template manually
echo -e "${YELLOW}Packaging template...${NC}"
cd infra

# First, validate the template
echo "Validating template..."
sam validate --template template.yaml

# Package without building (to avoid the validation error)
aws cloudformation package \
    --template-file template.yaml \
    --s3-bucket "$S3_BUCKET" \
    --output-template-file packaged-template.yaml \
    --region "$REGION"

# Step 5: Deploy using CloudFormation directly
echo -e "${GREEN}Deploying stack...${NC}"
aws cloudformation deploy \
    --template-file packaged-template.yaml \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND \
    --region "$REGION" \
    --parameter-overrides \
        ParameterKey=Environment,ParameterValue=prod

# Step 6: Get outputs
echo -e "${GREEN}Getting stack outputs...${NC}"
cd ..
outputs=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs' \
    --output json)

# Parse outputs
API_URL=$(echo "$outputs" | jq -r '.[] | select(.OutputKey=="ApiUrl") | .OutputValue')

echo -e "${GREEN}✅ Deployment complete!${NC}"
echo ""
echo "API Endpoint: $API_URL"
echo ""
echo "Test with:"
echo "curl -X POST $API_URL/files/presign -H 'Content-Type: application/json' -d '{\"filename\":\"test.xlsx\"}'"