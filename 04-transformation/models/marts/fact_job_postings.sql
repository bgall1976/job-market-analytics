{{
    config(
        materialized='table',
        tags=['marts', 'facts']
    )
}}

/*
    Fact table for job postings
    Central table for job market analysis
*/

WITH jobs AS (
    SELECT * FROM {{ ref('stg_jobs') }}
),

companies AS (
    SELECT * FROM {{ ref('stg_companies') }}
),

locations AS (
    SELECT * FROM {{ ref('dim_locations') }}
),

fact_jobs AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['j.job_id']) }} AS job_key,
        
        -- Natural key
        j.job_id,
        
        -- Foreign keys
        c.company_key,
        l.location_key,
        
        -- Job attributes
        j.title,
        j.title_normalized,
        j.job_type,
        j.is_remote,
        
        -- Compensation
        j.salary_min,
        j.salary_max,
        j.salary_midpoint,
        j.salary_currency,
        
        -- Skills
        j.skills,
        j.skill_count,
        
        -- Content
        j.description,
        j.url,
        
        -- Dates
        j.posted_date,
        j.scraped_at,
        
        -- Metadata
        j.data_source,
        j.processing_date,
        
        -- Derived metrics
        CASE 
            WHEN j.salary_midpoint >= 200000 THEN 'Executive'
            WHEN j.salary_midpoint >= 150000 THEN 'Senior'
            WHEN j.salary_midpoint >= 100000 THEN 'Mid-Level'
            WHEN j.salary_midpoint >= 60000 THEN 'Junior'
            ELSE 'Entry'
        END AS salary_band,
        
        -- Title classification
        CASE
            WHEN LOWER(j.title_normalized) LIKE '%senior%' 
                 OR LOWER(j.title_normalized) LIKE '%sr%'
                 OR LOWER(j.title_normalized) LIKE '%lead%'
                 OR LOWER(j.title_normalized) LIKE '%principal%'
            THEN 'Senior'
            WHEN LOWER(j.title_normalized) LIKE '%junior%'
                 OR LOWER(j.title_normalized) LIKE '%jr%'
                 OR LOWER(j.title_normalized) LIKE '%entry%'
                 OR LOWER(j.title_normalized) LIKE '%associate%'
            THEN 'Junior'
            WHEN LOWER(j.title_normalized) LIKE '%manager%'
                 OR LOWER(j.title_normalized) LIKE '%director%'
                 OR LOWER(j.title_normalized) LIKE '%head%'
                 OR LOWER(j.title_normalized) LIKE '%vp%'
            THEN 'Management'
            ELSE 'Mid-Level'
        END AS seniority_level,
        
        -- Role classification
        CASE
            WHEN LOWER(j.title_normalized) LIKE '%data engineer%' THEN 'Data Engineer'
            WHEN LOWER(j.title_normalized) LIKE '%analytics engineer%' THEN 'Analytics Engineer'
            WHEN LOWER(j.title_normalized) LIKE '%data scientist%' THEN 'Data Scientist'
            WHEN LOWER(j.title_normalized) LIKE '%data analyst%' THEN 'Data Analyst'
            WHEN LOWER(j.title_normalized) LIKE '%machine learning%' 
                 OR LOWER(j.title_normalized) LIKE '%ml engineer%' THEN 'ML Engineer'
            WHEN LOWER(j.title_normalized) LIKE '%platform%' THEN 'Platform Engineer'
            ELSE 'Other'
        END AS role_category

    FROM jobs j
    LEFT JOIN companies c ON j.company = c.company_name
    LEFT JOIN locations l ON j.city = l.city AND j.state = l.state
)

SELECT * FROM fact_jobs
