{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

/*
    Company dimension table
    Unique companies with derived attributes
*/

WITH companies AS (
    SELECT * FROM {{ ref('stg_companies') }}
),

company_jobs AS (
    SELECT
        company,
        COUNT(*) AS total_jobs,
        COUNT(DISTINCT role_category) AS role_diversity,
        AVG(salary_midpoint) AS avg_salary,
        MIN(salary_midpoint) AS min_salary,
        MAX(salary_midpoint) AS max_salary,
        SUM(CASE WHEN is_remote THEN 1 ELSE 0 END) AS remote_jobs,
        COUNT(DISTINCT city) AS city_count,
        MIN(posted_date) AS first_posting,
        MAX(posted_date) AS last_posting,
        -- Most common skills at company
        ARRAY_AGG(DISTINCT skill) WITHIN GROUP (ORDER BY skill_count DESC) AS top_skills
    FROM {{ ref('fact_job_postings') }} f,
    LATERAL FLATTEN(input => f.skills) AS s(skill)
    GROUP BY company
),

dim_companies AS (
    SELECT
        -- Keys
        c.company_key,
        c.company_name,
        c.company_name_upper,
        
        -- Size classification
        c.company_size_estimate,
        
        -- Job statistics
        COALESCE(cj.total_jobs, 0) AS total_job_postings,
        COALESCE(cj.role_diversity, 0) AS role_diversity,
        
        -- Compensation
        cj.avg_salary,
        cj.min_salary,
        cj.max_salary,
        
        -- Remote work
        COALESCE(cj.remote_jobs, 0) AS remote_job_count,
        CASE 
            WHEN cj.total_jobs > 0 
            THEN ROUND(100.0 * cj.remote_jobs / cj.total_jobs, 1)
            ELSE 0
        END AS remote_percentage,
        
        -- Geographic spread
        cj.city_count AS location_count,
        
        -- Activity
        cj.first_posting,
        cj.last_posting,
        c.last_seen_at,
        
        -- Top skills (first 10)
        SLICE(cj.top_skills, 1, 10) AS top_skills,
        
        -- Hiring velocity (jobs per month)
        CASE 
            WHEN DATEDIFF('month', cj.first_posting, cj.last_posting) > 0
            THEN ROUND(cj.total_jobs * 1.0 / DATEDIFF('month', cj.first_posting, cj.last_posting), 1)
            ELSE cj.total_jobs
        END AS hiring_velocity,
        
        -- Company tier
        CASE
            WHEN cj.avg_salary >= 180000 AND cj.total_jobs >= 50 THEN 'Tier 1'
            WHEN cj.avg_salary >= 150000 AND cj.total_jobs >= 20 THEN 'Tier 2'
            WHEN cj.avg_salary >= 120000 AND cj.total_jobs >= 5 THEN 'Tier 3'
            ELSE 'Tier 4'
        END AS company_tier

    FROM companies c
    LEFT JOIN company_jobs cj ON c.company_name = cj.company
)

SELECT * FROM dim_companies
ORDER BY total_job_postings DESC
