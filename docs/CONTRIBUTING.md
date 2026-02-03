# Contributing to Job Market Analytics

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## 🚀 Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/job-market-analytics.git`
3. Create a branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Submit a pull request

## 📁 Project Structure

```
job-market-analytics/
├── 01-infrastructure/    # Terraform IaC
├── 02-ingestion/         # Data scrapers & Airbyte
├── 03-processing/        # PySpark notebooks
├── 04-transformation/    # dbt models
├── 05-quality/           # Great Expectations
├── 06-orchestration/     # Airflow DAGs
└── 07-visualization/     # Streamlit dashboard
```

## 🔧 Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install dev dependencies
pip install black isort mypy pytest
```

## 📝 Code Style

- **Python**: Follow PEP 8, use Black for formatting
- **SQL**: Use lowercase keywords, 4-space indentation
- **Terraform**: Use `terraform fmt` before committing

```bash
# Format Python code
black .
isort .

# Format Terraform
cd 01-infrastructure && terraform fmt
```

## 🧪 Testing

```bash
# Run Python tests
pytest tests/

# Run dbt tests
cd 04-transformation && dbt test

# Run Great Expectations
cd 05-quality && python scripts/run_validations.py
```

## 📋 Pull Request Checklist

- [ ] Code follows project style guidelines
- [ ] Tests pass locally
- [ ] Documentation updated if needed
- [ ] Commit messages are clear and descriptive
- [ ] No sensitive data (API keys, credentials) in code

## 💡 Ideas for Contributions

### Easy (Good First Issues)
- Add new job sources to scrapers
- Improve dashboard visualizations
- Add more data quality expectations
- Documentation improvements

### Medium
- Add new dbt models for analysis
- Implement additional scrapers (Indeed, LinkedIn)
- Add unit tests for Python code
- Create GitHub Actions CI/CD pipeline

### Advanced
- Add streaming data support with Kafka
- Implement Delta Lake instead of Parquet
- Add ML model for salary prediction
- Create Terraform modules for reusability

## 🐛 Reporting Bugs

1. Check existing issues first
2. Use the bug report template
3. Include:
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (OS, Python version, etc.)

## 💬 Questions?

- Open a Discussion on GitHub
- Tag your issue with `question`

## 📜 License

By contributing, you agree that your contributions will be licensed under the MIT License.
