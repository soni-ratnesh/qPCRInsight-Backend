#!/bin/bash

echo "📦 Setting up Lambda requirements..."

# Minimal requirements for simple Lambdas
cat > lambdas/upload_presign/requirements.txt << EOF
boto3==1.28.0
pydantic-settings>=2.0.0
EOF

cat > lambdas/ingest_queue/requirements.txt << EOF
boto3==1.28.0
pydantic-settings>=2.0.0
EOF

cat > lambdas/submit_job/requirements.txt << EOF
boto3==1.28.0
pydantic-settings>=2.0.0
EOF

cat > lambdas/get_job_status/requirements.txt << EOF
boto3==1.28.0
pydantic-settings>=2.0.0
EOF

cat > lambdas/download_results/requirements.txt << EOF
boto3==1.28.0
pydantic-settings>=2.0.0
EOF

cat > lambdas/notify_complete/requirements.txt << EOF
boto3==1.28.0
pydantic-settings>=2.0.0
EOF

# Stats worker needs statistical packages
cat > lambdas/stats_worker/requirements.txt << EOF
boto3==1.28.0
pandas==2.0.3
numpy==1.24.4
scipy==1.10.1
statsmodels==0.14.0
pydantic-settings>=2.0.0
EOF

# Plot worker needs plotting packages
cat > lambdas/plot_worker/requirements.txt << EOF
boto3==1.28.0
pandas==2.0.3
numpy==1.24.4
plotly==5.14.1
kaleido==0.2.1
pydantic-settings>=2.0.0
EOF

# Report builder needs report generation packages
cat > lambdas/report_builder/requirements.txt << EOF
boto3==1.28.0
pandas==2.0.3
numpy==1.24.4
xlsxwriter==3.1.2
reportlab==4.0.4
pydantic-settings>=2.0.0
EOF

cat > lambdas/parse_file/requirements.txt << EOF
boto3==1.28.0
pandas==2.0.3
numpy==1.24.4
openpyxl==3.1.2
pydantic-settings>=2.0.0
EOF

# Quality Control Lambda
cat > lambdas/quality_control/requirements.txt << EOF
boto3==1.28.0
pandas==2.0.3
numpy==1.24.4
pydantic-settings>=2.0.0
EOF

# Normalize Data Lambda
cat > lambdas/normalize_data/requirements.txt << EOF
boto3==1.28.0
pandas==2.0.3
numpy==1.24.4
pydantic-settings>=2.0.0
EOF

# Fold Change Lambda
cat > lambdas/fold_change/requirements.txt << EOF
boto3==1.28.0
pandas==2.0.3
numpy==1.24.4
scipy==1.10.1
pydantic-settings>=2.0.0
EOF

# Stats Worker Lambda
cat > lambdas/stats_worker/requirements.txt << EOF
boto3==1.28.0
pandas==2.0.3
numpy==1.24.4
scipy==1.10.1
statsmodels==0.14.0
pydantic-settings>=2.0.0
EOF

# Plot Worker Lambda
cat > lambdas/plot_worker/requirements.txt << EOF
boto3==1.28.0
pandas==2.0.3
numpy==1.24.4
plotly==5.14.1
kaleido==0.2.1
pydantic-settings>=2.0.0
EOF

# Report Builder Lambda
cat > lambdas/report_builder/requirements.txt << EOF
boto3==1.28.0
pandas==2.0.3
numpy==1.24.4
xlsxwriter==3.1.2
reportlab==4.0.4
pydantic-settings>=2.0.0
EOF

# Notify Complete Lambda
cat > lambdas/notify_complete/requirements.txt << EOF
boto3==1.28.0
pydantic-settings>=2.0.0
EOF

# Error Handler Lambda
cat > lambdas/error_handler/requirements.txt << EOF
boto3==1.28.0
pydantic-settings>=2.0.0
EOF

# Update existing Lambda requirements
cat > lambdas/analysis_runner/requirements.txt << EOF
boto3==1.28.0
pydantic-settings>=2.0.0
EOF


echo "✅ Lambda requirements files created"