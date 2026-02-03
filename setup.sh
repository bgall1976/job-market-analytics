#!/bin/bash
# =============================================================================
# Job Market Analytics - Quick Setup Script
# =============================================================================
#
# Usage:
#   chmod +x setup.sh
#   ./setup.sh
#
# This script will:
#   1. Create Python virtual environment
#   2. Install dependencies
#   3. Set up environment variables
#   4. Initialize Terraform
#   5. Start local services
#
# =============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Job Market Analytics - Setup Script  ${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# -----------------------------------------------------------------------------
# Check prerequisites
# -----------------------------------------------------------------------------
echo -e "${YELLOW}Checking prerequisites...${NC}"

# Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is required but not installed.${NC}"
    exit 1
fi
echo "✓ Python 3 found: $(python3 --version)"

# pip
if ! command -v pip3 &> /dev/null; then
    echo -e "${RED}Error: pip3 is required but not installed.${NC}"
    exit 1
fi
echo "✓ pip3 found"

# Docker (optional)
if command -v docker &> /dev/null; then
    echo "✓ Docker found: $(docker --version | cut -d' ' -f3)"
    DOCKER_AVAILABLE=true
else
    echo "⚠ Docker not found (optional, needed for Airflow/Airbyte)"
    DOCKER_AVAILABLE=false
fi

# Terraform (optional)
if command -v terraform &> /dev/null; then
    echo "✓ Terraform found: $(terraform --version | head -n1)"
    TERRAFORM_AVAILABLE=true
else
    echo "⚠ Terraform not found (optional, needed for AWS infrastructure)"
    TERRAFORM_AVAILABLE=false
fi

# AWS CLI (optional)
if command -v aws &> /dev/null; then
    echo "✓ AWS CLI found"
    AWS_AVAILABLE=true
else
    echo "⚠ AWS CLI not found (optional, needed for S3 access)"
    AWS_AVAILABLE=false
fi

echo ""

# -----------------------------------------------------------------------------
# Create virtual environment
# -----------------------------------------------------------------------------
echo -e "${YELLOW}Creating Python virtual environment...${NC}"

if [ -d "venv" ]; then
    echo "Virtual environment already exists, skipping..."
else
    python3 -m venv venv
    echo "✓ Virtual environment created"
fi

# Activate virtual environment
source venv/bin/activate
echo "✓ Virtual environment activated"

# -----------------------------------------------------------------------------
# Install dependencies
# -----------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}Installing Python dependencies...${NC}"

pip install --upgrade pip
pip install -r requirements.txt

echo "✓ Dependencies installed"

# -----------------------------------------------------------------------------
# Create .env file
# -----------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}Setting up environment variables...${NC}"

if [ -f ".env" ]; then
    echo ".env file already exists, skipping..."
else
    cat > .env << 'EOF'
# =============================================================================
# Job Market Analytics - Environment Variables
# =============================================================================
# Copy this file to .env and fill in your values

# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1

# S3 Buckets (will be created by Terraform)
S3_BRONZE_BUCKET=job-analytics-bronze-xxxx
S3_SILVER_BUCKET=job-analytics-silver-xxxx
S3_GOLD_BUCKET=job-analytics-gold-xxxx

# RapidAPI (for job scraping)
RAPIDAPI_KEY=your_rapidapi_key_here

# Slack Notifications (optional)
SLACK_WEBHOOK_URL=

# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Local Development
USE_LOCAL_DATA=true
LOCAL_DATA_PATH=./data/
EOF
    echo "✓ Created .env template"
    echo -e "${YELLOW}  ⚠ Please edit .env with your credentials${NC}"
fi

# -----------------------------------------------------------------------------
# Create data directories
# -----------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}Creating data directories...${NC}"

mkdir -p data/{raw,bronze,silver,gold}
echo "✓ Data directories created"

# -----------------------------------------------------------------------------
# Initialize Terraform (if available)
# -----------------------------------------------------------------------------
if [ "$TERRAFORM_AVAILABLE" = true ]; then
    echo ""
    echo -e "${YELLOW}Initializing Terraform...${NC}"
    
    cd 01-infrastructure
    terraform init
    cd ..
    
    echo "✓ Terraform initialized"
    echo -e "${YELLOW}  Run 'cd 01-infrastructure && terraform plan' to preview infrastructure${NC}"
fi

# -----------------------------------------------------------------------------
# Initialize dbt
# -----------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}Installing dbt dependencies...${NC}"

cd 04-transformation
if [ -f "packages.yml" ]; then
    dbt deps 2>/dev/null || echo "  (dbt deps will run when profiles are configured)"
fi
cd ..

# -----------------------------------------------------------------------------
# Initialize Great Expectations
# -----------------------------------------------------------------------------
echo ""
echo -e "${YELLOW}Setting up Great Expectations...${NC}"

mkdir -p 05-quality/great_expectations/uncommitted/{validations,data_docs}
echo "✓ Great Expectations directories created"

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Setup Complete!                       ${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Next steps:"
echo ""
echo "1. Edit .env with your credentials:"
echo "   ${YELLOW}nano .env${NC}"
echo ""
echo "2. Deploy AWS infrastructure:"
echo "   ${YELLOW}cd 01-infrastructure && terraform plan && terraform apply${NC}"
echo ""
echo "3. Start Airflow (requires Docker):"
echo "   ${YELLOW}cd 06-orchestration && docker-compose up -d${NC}"
echo ""
echo "4. Run the Streamlit dashboard:"
echo "   ${YELLOW}cd 07-visualization && streamlit run app/main.py${NC}"
echo ""
echo "5. Test the scraper:"
echo "   ${YELLOW}python 02-ingestion/scrapers/job_scraper.py --test${NC}"
echo ""
echo -e "${GREEN}Happy data engineering! 🚀${NC}"
