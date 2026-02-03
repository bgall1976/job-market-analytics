#!/usr/bin/env python3
"""
Job Market Scraper
Extracts job postings from various career pages and APIs.

Usage:
    python job_scraper.py                           # Scrape all sources
    python job_scraper.py --source rapidapi         # Specific source
    python job_scraper.py --output s3://bucket/     # Output to S3
    python job_scraper.py --test                    # Dry run
"""

import argparse
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import boto3
import requests
import yaml
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class JobPosting:
    """Standardized job posting schema."""
    job_id: str
    title: str
    company: str
    location: str
    description: str
    source: str
    scraped_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    posted_date: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: str = "USD"
    job_type: Optional[str] = None  # full-time, contract, etc.
    remote: Optional[bool] = None
    url: Optional[str] = None
    skills: list = field(default_factory=list)
    raw_data: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)


class BaseScraper:
    """Base class for all scrapers."""

    def __init__(self, config: dict):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; JobAnalytics/1.0)'
        })

    def scrape(self) -> list[JobPosting]:
        raise NotImplementedError

    def generate_job_id(self, *args) -> str:
        """Generate deterministic job ID from inputs."""
        content = '|'.join(str(a) for a in args)
        return hashlib.md5(content.encode()).hexdigest()[:16]

    def extract_salary(self, text: str) -> tuple[Optional[float], Optional[float]]:
        """Extract salary range from text."""
        if not text:
            return None, None

        # Pattern: $150,000 - $200,000 or $150K-$200K
        patterns = [
            r'\$?([\d,]+)k?\s*[-–to]+\s*\$?([\d,]+)k?',
            r'\$?([\d,]+)\s*[-–to]+\s*\$?([\d,]+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                min_sal = float(match.group(1).replace(',', ''))
                max_sal = float(match.group(2).replace(',', ''))
                # Handle K notation
                if min_sal < 1000:
                    min_sal *= 1000
                if max_sal < 1000:
                    max_sal *= 1000
                return min_sal, max_sal

        return None, None

    def extract_skills(self, text: str) -> list[str]:
        """Extract technical skills from job description."""
        skills_keywords = [
            'python', 'sql', 'spark', 'kafka', 'airflow', 'dbt',
            'aws', 'azure', 'gcp', 'snowflake', 'databricks',
            'docker', 'kubernetes', 'terraform', 'git',
            'java', 'scala', 'go', 'rust',
            'postgresql', 'mysql', 'mongodb', 'redis',
            'tableau', 'power bi', 'looker',
            'machine learning', 'ml', 'ai', 'deep learning',
            'etl', 'elt', 'data warehouse', 'data lake',
            'pandas', 'numpy', 'scikit-learn', 'tensorflow', 'pytorch'
        ]

        text_lower = text.lower()
        found_skills = []

        for skill in skills_keywords:
            if skill in text_lower:
                found_skills.append(skill)

        return list(set(found_skills))


class RapidAPIScraper(BaseScraper):
    """Scraper for RapidAPI JSearch endpoint."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.api_key = os.environ.get('RAPIDAPI_KEY', config.get('api_key', ''))
        self.base_url = "https://jsearch.p.rapidapi.com"

    def scrape(self) -> list[JobPosting]:
        if not self.api_key:
            logger.warning("RAPIDAPI_KEY not set, skipping RapidAPI scraper")
            return []

        jobs = []
        queries = self.config.get('queries', ['data engineer'])

        for query in queries:
            try:
                jobs.extend(self._search_jobs(query))
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logger.error(f"Error searching for '{query}': {e}")

        return jobs

    def _search_jobs(self, query: str) -> list[JobPosting]:
        """Search for jobs using JSearch API."""
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
        }

        params = {
            "query": query,
            "page": "1",
            "num_pages": "1",
            "date_posted": "month"
        }

        response = self.session.get(
            f"{self.base_url}/search",
            headers=headers,
            params=params
        )
        response.raise_for_status()
        data = response.json()

        jobs = []
        for item in data.get('data', []):
            salary_min, salary_max = self.extract_salary(
                item.get('job_salary', '') or item.get('job_description', '')
            )

            job = JobPosting(
                job_id=self.generate_job_id(item.get('job_id', ''), item.get('employer_name', '')),
                title=item.get('job_title', ''),
                company=item.get('employer_name', ''),
                location=f"{item.get('job_city', '')}, {item.get('job_state', '')}",
                description=item.get('job_description', ''),
                source='rapidapi',
                posted_date=item.get('job_posted_at_datetime_utc'),
                salary_min=salary_min,
                salary_max=salary_max,
                job_type=item.get('job_employment_type', ''),
                remote=item.get('job_is_remote', False),
                url=item.get('job_apply_link', ''),
                skills=self.extract_skills(item.get('job_description', '')),
                raw_data=item
            )
            jobs.append(job)

        logger.info(f"Found {len(jobs)} jobs for query: {query}")
        return jobs


class GreenhouseScraper(BaseScraper):
    """Scraper for Greenhouse job boards."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.companies = config.get('companies', [])

    def scrape(self) -> list[JobPosting]:
        jobs = []

        for company in self.companies:
            try:
                company_jobs = self._scrape_company(company)
                jobs.extend(company_jobs)
                time.sleep(2)  # Be respectful
            except Exception as e:
                logger.error(f"Error scraping {company}: {e}")

        return jobs

    def _scrape_company(self, company_slug: str) -> list[JobPosting]:
        """Scrape jobs from a Greenhouse board."""
        url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs"

        response = self.session.get(url)
        response.raise_for_status()
        data = response.json()

        jobs = []
        for item in data.get('jobs', []):
            # Filter for data/engineering roles
            title_lower = item.get('title', '').lower()
            if not any(kw in title_lower for kw in ['data', 'engineer', 'analytics']):
                continue

            # Get full job details
            detail_url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs/{item['id']}"
            detail_response = self.session.get(detail_url)
            detail_data = detail_response.json() if detail_response.ok else {}

            description = BeautifulSoup(
                detail_data.get('content', ''),
                'html.parser'
            ).get_text()

            salary_min, salary_max = self.extract_salary(description)

            location = item.get('location', {}).get('name', 'Unknown')

            job = JobPosting(
                job_id=self.generate_job_id(item['id'], company_slug),
                title=item.get('title', ''),
                company=company_slug.replace('-', ' ').title(),
                location=location,
                description=description,
                source='greenhouse',
                posted_date=item.get('updated_at'),
                salary_min=salary_min,
                salary_max=salary_max,
                remote='remote' in location.lower(),
                url=item.get('absolute_url', ''),
                skills=self.extract_skills(description),
                raw_data=item
            )
            jobs.append(job)

        logger.info(f"Found {len(jobs)} data/engineering jobs at {company_slug}")
        return jobs


class LeverScraper(BaseScraper):
    """Scraper for Lever job boards."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.companies = config.get('companies', [])

    def scrape(self) -> list[JobPosting]:
        jobs = []

        for company in self.companies:
            try:
                company_jobs = self._scrape_company(company)
                jobs.extend(company_jobs)
                time.sleep(2)
            except Exception as e:
                logger.error(f"Error scraping {company}: {e}")

        return jobs

    def _scrape_company(self, company_slug: str) -> list[JobPosting]:
        """Scrape jobs from a Lever board."""
        url = f"https://api.lever.co/v0/postings/{company_slug}"

        response = self.session.get(url)
        response.raise_for_status()
        data = response.json()

        jobs = []
        for item in data:
            title_lower = item.get('text', '').lower()
            if not any(kw in title_lower for kw in ['data', 'engineer', 'analytics']):
                continue

            description = item.get('descriptionPlain', '')
            salary_min, salary_max = self.extract_salary(
                item.get('salaryRange', {}).get('text', '') or description
            )

            location = item.get('categories', {}).get('location', 'Unknown')

            job = JobPosting(
                job_id=self.generate_job_id(item['id'], company_slug),
                title=item.get('text', ''),
                company=company_slug.replace('-', ' ').title(),
                location=location,
                description=description,
                source='lever',
                posted_date=datetime.fromtimestamp(
                    item.get('createdAt', 0) / 1000,
                    tz=timezone.utc
                ).isoformat() if item.get('createdAt') else None,
                salary_min=salary_min,
                salary_max=salary_max,
                remote='remote' in location.lower(),
                url=item.get('hostedUrl', ''),
                skills=self.extract_skills(description),
                raw_data=item
            )
            jobs.append(job)

        logger.info(f"Found {len(jobs)} data/engineering jobs at {company_slug}")
        return jobs


class OutputHandler:
    """Handle output to various destinations."""

    def __init__(self, output_path: str):
        self.output_path = output_path
        self.is_s3 = output_path.startswith('s3://')

        if self.is_s3:
            self.s3_client = boto3.client('s3')
            # Parse s3://bucket/prefix
            parts = output_path[5:].split('/', 1)
            self.bucket = parts[0]
            self.prefix = parts[1] if len(parts) > 1 else ''
        else:
            Path(output_path).mkdir(parents=True, exist_ok=True)

    def write(self, jobs: list[JobPosting], source: str):
        """Write jobs to output destination."""
        if not jobs:
            logger.warning(f"No jobs to write for source: {source}")
            return

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        filename = f"{source}_{timestamp}.jsonl"

        content = '\n'.join(job.to_json() for job in jobs)

        if self.is_s3:
            key = f"{self.prefix}{filename}"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content.encode('utf-8'),
                ContentType='application/x-ndjson'
            )
            logger.info(f"Wrote {len(jobs)} jobs to s3://{self.bucket}/{key}")
        else:
            filepath = Path(self.output_path) / filename
            filepath.write_text(content)
            logger.info(f"Wrote {len(jobs)} jobs to {filepath}")


def load_config(config_path: str = None) -> dict:
    """Load scraper configuration."""
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            return yaml.safe_load(f)

    # Default configuration
    return {
        'rapidapi': {
            'enabled': True,
            'queries': [
                'data engineer',
                'data engineering',
                'analytics engineer',
                'data platform engineer'
            ]
        },
        'greenhouse': {
            'enabled': True,
            'companies': [
                'anthropic',
                'openai',
                'databricks',
                'snowflake',
                'stripe',
                'airbnb'
            ]
        },
        'lever': {
            'enabled': True,
            'companies': [
                'netflix',
                'figma',
                'notion'
            ]
        }
    }


def main():
    parser = argparse.ArgumentParser(description='Job Market Scraper')
    parser.add_argument('--config', '-c', help='Config file path')
    parser.add_argument('--output', '-o', default='./data/raw',
                        help='Output path (local or s3://)')
    parser.add_argument('--source', '-s', choices=['rapidapi', 'greenhouse', 'lever', 'all'],
                        default='all', help='Source to scrape')
    parser.add_argument('--test', action='store_true', help='Dry run without writing')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = load_config(args.config)
    output = OutputHandler(args.output) if not args.test else None

    scrapers = {
        'rapidapi': RapidAPIScraper(config.get('rapidapi', {})),
        'greenhouse': GreenhouseScraper(config.get('greenhouse', {})),
        'lever': LeverScraper(config.get('lever', {}))
    }

    total_jobs = 0
    sources_to_run = [args.source] if args.source != 'all' else scrapers.keys()

    for source in sources_to_run:
        if source not in scrapers:
            continue

        if not config.get(source, {}).get('enabled', True):
            logger.info(f"Skipping disabled source: {source}")
            continue

        logger.info(f"Starting scraper: {source}")
        try:
            jobs = scrapers[source].scrape()
            total_jobs += len(jobs)

            if args.test:
                logger.info(f"[TEST] Would write {len(jobs)} jobs from {source}")
                for job in jobs[:3]:  # Show sample
                    logger.info(f"  - {job.title} at {job.company}")
            elif output:
                output.write(jobs, source)

        except Exception as e:
            logger.error(f"Failed to scrape {source}: {e}")

    logger.info(f"Scraping complete. Total jobs: {total_jobs}")


if __name__ == '__main__':
    main()
