{{
    config(
        materialized='view',
        tags=['staging', 'companies']
    )
}}

/*
    Staging model for unique companies
    Derived from job postings
*/

WITH job_companies AS (
    SELECT DISTINCT
        company,
        -- Get latest posting info per company
        MAX(scraped_at) AS last_seen_at,
        COUNT(*) AS total_postings,
        COUNT(DISTINCT city) AS location_count,
        AVG(salary_midpoint) AS avg_salary
    FROM {{ ref('stg_jobs') }}
    GROUP BY company
),

staged AS (
    SELECT
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['company']) }} AS company_key,
        
        -- Company name (normalized)
        company AS company_name,
        UPPER(company) AS company_name_upper,
        LOWER(company) AS company_name_lower,
        
        -- Derived metrics
        total_postings,
        location_count,
        avg_salary,
        
        -- Activity tracking
        last_seen_at,
        
        -- Size estimation based on posting volume
        CASE
            WHEN total_postings >= 50 THEN 'Enterprise'
            WHEN total_postings >= 20 THEN 'Large'
            WHEN total_postings >= 5 THEN 'Medium'
            ELSE 'Small'
        END AS company_size_estimate

    FROM job_companies
)

SELECT * FROM staged
