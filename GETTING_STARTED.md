# 🚀 Getting Started Guide

A complete step-by-step guide to clone this repository and build the Job Market Analytics pipeline from scratch.

**Estimated Time:** 2-3 hours for full setup

---

## 📋 Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Clone & Initial Setup](#2-clone--initial-setup)
3. [Step 1: Deploy Infrastructure](#3-step-1-deploy-infrastructure)
4. [Step 2: Configure Data Ingestion](#4-step-2-configure-data-ingestion)
5. [Step 3: Set Up Data Processing](#5-step-3-set-up-data-processing)
6. [Step 4: Configure dbt Transformations](#6-step-4-configure-dbt-transformations)
7. [Step 5: Set Up Data Quality](#7-step-5-set-up-data-quality)
8. [Step 6: Deploy Orchestration](#8-step-6-deploy-orchestration)
9. [Step 7: Launch Dashboard](#9-step-7-launch-dashboard)
10. [Running the Full Pipeline](#10-running-the-full-pipeline)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Prerequisites

### Required Accounts (All Free Tier)

| Service | Sign Up Link | What You'll Need |
|---------|--------------|------------------|
| **AWS** | [aws.amazon.com/free](https://aws.amazon.com/free) | Access Key & Secret |
| **Databricks Community** | [community.cloud.databricks.com](https://community.cloud.databricks.com/login.html) | Free account |
| **GitHub** | [github.com](https://github.com) | To fork/clone repo |
| **RapidAPI** (optional) | [rapidapi.com](https://rapidapi.com) | For job API access |

### Required Software

```bash
# Check if you have these installed:
python3 --version    # Need 3.10+
docker --version     # Need 20.0+
terraform --version  # Need 1.0+
aws --version        # AWS CLI v2
git --version        # Any recent version
```

### Install Missing Software

**macOS:**
```bash
# Install Homebrew if needed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install requirements
brew install python@3.11 terraform awscli git
brew install --cask docker
```

**Windows:**
```powershell
# Using Chocolatey (run as Administrator)
choco install python311 terraform awscli git docker-desktop
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install python3.11 python3.11-venv git curl unzip

# Install Terraform
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
sudo apt install terraform

# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install

# Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
```

---

## 2. Clone & Initial Setup

### 2.1 Clone the Repository

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/job-market-analytics.git
cd job-market-analytics

# View project structure
ls -la
```

### 2.2 Create Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate  # macOS/Linux
# OR
.\venv\Scripts\activate   # Windows

# Verify activation
which python  # Should show path to venv
```

### 2.3 Install Python Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 2.4 Configure Environment Variables

```bash
# Copy the template
cp .env.example .env

# Edit with your credentials
nano .env  # or use your preferred editor
```

**Fill in your `.env` file:**
```bash
# AWS Configuration (get from AWS Console → IAM → Security Credentials)
AWS_ACCESS_KEY_ID=AKIA...your_key...
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1

# RapidAPI (optional - get from rapidapi.com dashboard)
RAPIDAPI_KEY=your_rapidapi_key_here

# Leave these empty for now - Terraform will create the buckets
S3_BRONZE_BUCKET=
S3_SILVER_BUCKET=
S3_GOLD_BUCKET=
```

### 2.5 Configure AWS CLI

```bash
aws configure
# Enter:
#   AWS Access Key ID: [your key]
#   AWS Secret Access Key: [your secret]
#   Default region name: us-east-1
#   Default output format: json

# Verify configuration
aws sts get-caller-identity
```

---

## 3. Step 1: Deploy Infrastructure

### 3.1 Initialize Terraform

```bash
cd 01-infrastructure

# Initialize Terraform (downloads AWS provider)
terraform init
```

**Expected output:**
```
Terraform has been successfully initialized!
```

### 3.2 Preview Infrastructure

```bash
# See what will be created
terraform plan
```

**Review the plan - you should see:**
- 3 S3 buckets (bronze, silver, gold)
- 1 Glue database
- 3 Glue crawlers
- IAM roles and policies

### 3.3 Deploy Infrastructure

```bash
# Create all resources
terraform apply

# Type 'yes' when prompted
```

**This creates:**
- `job-analytics-bronze-XXXX` - Raw data bucket
- `job-analytics-silver-XXXX` - Cleaned data bucket  
- `job-analytics-gold-XXXX` - Aggregated data bucket
- Glue Data Catalog and crawlers

### 3.4 Save Outputs

```bash
# View created resources
terraform output

# Save bucket names to use later
terraform output -json > ../config/infrastructure.json

# Copy bucket names to your .env file
terraform output s3_bucket_bronze
terraform output s3_bucket_silver
terraform output s3_bucket_gold
```

**Update `.env` with actual bucket names:**
```bash
S3_BRONZE_BUCKET=job-analytics-bronze-a1b2c3d4
S3_SILVER_BUCKET=job-analytics-silver-a1b2c3d4
S3_GOLD_BUCKET=job-analytics-gold-a1b2c3d4
```

```bash
cd ..  # Return to project root
```

---

## 4. Step 2: Configure Data Ingestion

### 4.1 Test the Scraper (No API Key Needed)

```bash
cd 02-ingestion/scrapers

# Test with Greenhouse (free, no API key)
python job_scraper.py --source greenhouse --test
```

**Expected output:**
```
Starting scraper: greenhouse
Found 12 data/engineering jobs at anthropic
Found 8 data/engineering jobs at databricks
...
[TEST] Would write 45 jobs from greenhouse
```

### 4.2 Run Scraper with Local Output

```bash
# Create local data directory
mkdir -p ../../data/raw

# Scrape and save locally
python job_scraper.py --source greenhouse --output ../../data/raw/
```

### 4.3 Run Scraper with S3 Output

```bash
# Load environment variables
source ../../.env

# Scrape directly to S3
python job_scraper.py --source greenhouse \
    --output s3://${S3_BRONZE_BUCKET}/job_postings/greenhouse/
```

### 4.4 (Optional) Set Up Airbyte

```bash
cd ..  # Back to 02-ingestion

# Start Airbyte (requires Docker)
docker-compose up -d

# Wait 2-3 minutes for startup
docker-compose logs -f  # Press Ctrl+C to exit

# Access Airbyte UI
open http://localhost:8000  # macOS
# OR visit http://localhost:8000 in browser
```

**In Airbyte UI:**
1. Create Source → Google Sheets or HTTP API
2. Create Destination → S3 (use your bronze bucket)
3. Create Connection between source and destination

```bash
cd ..  # Return to project root
```

---

## 5. Step 3: Set Up Data Processing

### 5.1 Option A: Local PySpark (Quick Test)

```bash
cd 03-processing

# Install PySpark locally
pip install pyspark

# Run processing on local data
python notebooks/01_bronze_to_silver.py \
    --input ../data/raw/ \
    --output ../data/silver/
```

### 5.2 Option B: Databricks Community Edition (Recommended)

**Step 1: Log in to Databricks**
1. Go to [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Create account or sign in

**Step 2: Create a Cluster**
1. Click "Compute" in sidebar
2. Click "Create Cluster"
3. Settings:
   - Name: `job-analytics`
   - Runtime: `13.3 LTS (Spark 3.4.1, Scala 2.12)`
   - Node type: Default
4. Click "Create Cluster"
5. Wait for cluster to start (2-3 minutes)

**Step 3: Import Notebook**
1. Click "Workspace" in sidebar
2. Click "Import"
3. Upload `03-processing/notebooks/01_bronze_to_silver.py`

**Step 4: Configure S3 Access**

Add this cell at the top of notebook:
```python
# Configure S3 access
spark.conf.set("fs.s3a.access.key", "YOUR_ACCESS_KEY")
spark.conf.set("fs.s3a.secret.key", "YOUR_SECRET_KEY")

# Update paths
CONFIG = {
    "bronze_path": "s3a://job-analytics-bronze-XXXX/job_postings/",
    "silver_path": "s3a://job-analytics-silver-XXXX/jobs_cleaned/",
}
```

**Step 5: Run Notebook**
1. Attach notebook to your cluster
2. Run all cells (Shift+Enter or Run All)

```bash
cd ..  # Return to project root
```

---

## 6. Step 4: Configure dbt Transformations

### 6.1 Set Up dbt Profile

```bash
cd 04-transformation

# Create dbt profiles directory
mkdir -p ~/.dbt

# Create profiles.yml
cat > ~/.dbt/profiles.yml << 'EOF'
job_analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ./data/job_analytics.duckdb
      threads: 4
EOF
```

### 6.2 Install dbt Dependencies

```bash
# Install dbt packages
dbt deps
```

### 6.3 Test Connection

```bash
dbt debug
```

**Expected output:**
```
All checks passed!
```

### 6.4 Load Seed Data

```bash
# Load reference data (skill categories)
dbt seed
```

### 6.5 Run Transformations

```bash
# Run all models
dbt run

# Expected: Creates staging and mart tables
```

### 6.6 Run Tests

```bash
# Validate data quality
dbt test
```

### 6.7 Generate Documentation

```bash
# Generate and view docs
dbt docs generate
dbt docs serve

# Opens browser at http://localhost:8080
```

```bash
cd ..  # Return to project root
```

---

## 7. Step 5: Set Up Data Quality

### 7.1 Initialize Great Expectations

```bash
cd 05-quality

# The great_expectations folder is already configured
# Just verify the setup
ls great_expectations/
```

### 7.2 Run Validations

```bash
# Run validation script
python scripts/run_validations.py --checkpoint silver_checkpoint
```

### 7.3 View Data Docs

```bash
# Build HTML reports
cd great_expectations
great_expectations docs build

# Open in browser
open uncommitted/data_docs/local_site/index.html  # macOS
# OR
xdg-open uncommitted/data_docs/local_site/index.html  # Linux
```

```bash
cd ../..  # Return to project root
```

---

## 8. Step 6: Deploy Orchestration

### 8.1 Set Up Airflow

```bash
cd 06-orchestration

# Create required directories
mkdir -p logs plugins

# Set Airflow UID (Linux only)
echo "AIRFLOW_UID=$(id -u)" >> .env

# Initialize Airflow database
docker-compose up airflow-init

# Start all services
docker-compose up -d
```

### 8.2 Access Airflow UI

```bash
# Wait 30 seconds for startup
sleep 30

# Check services are running
docker-compose ps

# Open Airflow
open http://localhost:8080  # macOS
```

**Login credentials:**
- Username: `airflow`
- Password: `airflow`

### 8.3 Enable the DAG

1. In Airflow UI, find `job_analytics_pipeline`
2. Toggle the switch to enable it
3. Click the play button to trigger manually

### 8.4 Monitor Execution

1. Click on the DAG name
2. View task status in Graph view
3. Click tasks to see logs

```bash
cd ..  # Return to project root
```

---

## 9. Step 7: Launch Dashboard

### 9.1 Run Locally

```bash
cd 07-visualization

# Set to use local sample data
export USE_LOCAL_DATA=true

# Start Streamlit
streamlit run app/main.py
```

**Dashboard opens at:** http://localhost:8501

### 9.2 Deploy to Streamlit Cloud (Free)

**Step 1: Push to GitHub**
```bash
cd ..  # Project root
git add .
git commit -m "Complete pipeline setup"
git push origin main
```

**Step 2: Deploy on Streamlit Cloud**
1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Click "New app"
3. Select your repository
4. Set main file path: `07-visualization/app/main.py`
5. Click "Deploy"

**Step 3: Configure Secrets**

In Streamlit Cloud dashboard, add secrets:
```toml
AWS_ACCESS_KEY_ID = "your_key"
AWS_SECRET_ACCESS_KEY = "your_secret"
S3_GOLD_BUCKET = "job-analytics-gold-XXXX"
USE_LOCAL_DATA = "false"
```

---

## 10. Running the Full Pipeline

### Manual Execution (Step by Step)

```bash
# 1. Scrape new data
cd 02-ingestion/scrapers
python job_scraper.py --output s3://${S3_BRONZE_BUCKET}/job_postings/

# 2. Process data (run in Databricks or locally)
cd ../../03-processing
python notebooks/01_bronze_to_silver.py

# 3. Run dbt transformations
cd ../04-transformation
dbt run

# 4. Validate quality
cd ../05-quality
python scripts/run_validations.py

# 5. Update Glue catalog
aws glue start-crawler --name job-analytics-silver-crawler
aws glue start-crawler --name job-analytics-gold-crawler

# 6. Refresh dashboard
cd ../07-visualization
streamlit run app/main.py
```

### Automated Execution (Airflow)

```bash
cd 06-orchestration

# Trigger the full pipeline
docker-compose exec airflow-webserver \
    airflow dags trigger job_analytics_pipeline

# Monitor progress
docker-compose exec airflow-webserver \
    airflow tasks list job_analytics_pipeline --tree
```

---

## 11. Troubleshooting

### Common Issues

**Issue: Terraform "Access Denied"**
```bash
# Verify AWS credentials
aws sts get-caller-identity

# Reconfigure if needed
aws configure
```

**Issue: Docker containers won't start**
```bash
# Check Docker is running
docker ps

# Restart Docker Desktop, then:
docker-compose down
docker-compose up -d
```

**Issue: dbt "Profile not found"**
```bash
# Verify profiles.yml exists
cat ~/.dbt/profiles.yml

# Check you're in the right directory
pwd  # Should be in 04-transformation
```

**Issue: Databricks S3 access denied**
```python
# Make sure credentials are set correctly in notebook
spark.conf.set("fs.s3a.access.key", "YOUR_ACTUAL_KEY")
spark.conf.set("fs.s3a.secret.key", "YOUR_ACTUAL_SECRET")
```

**Issue: Streamlit "No data"**
```bash
# Use local sample data for testing
export USE_LOCAL_DATA=true
streamlit run app/main.py
```

### Getting Help

- 📖 Check component READMEs in each folder
- 🐛 Open an issue on GitHub
- 💬 Discussions tab for questions

---

## 🎉 Congratulations!

You've built a complete data engineering pipeline using:

- ✅ **Terraform** for infrastructure as code
- ✅ **S3** for data lake storage
- ✅ **PySpark/Databricks** for data processing
- ✅ **dbt** for SQL transformations
- ✅ **Great Expectations** for data quality
- ✅ **Airflow** for orchestration
- ✅ **Streamlit** for visualization

All using **free tier services**!

---

## 📚 Next Steps

1. **Customize scrapers** - Add more job sources in `02-ingestion/scrapers/companies.yaml`
2. **Add more dbt models** - Create new analyses in `04-transformation/models/`
3. **Enhance dashboard** - Add new charts in `07-visualization/app/`
4. **Set up alerts** - Configure Slack notifications in Airflow
5. **Add CI/CD** - Set up GitHub Actions for automated testing
