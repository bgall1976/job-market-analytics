{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

/*
    Location dimension table
    Unique locations derived from job postings
*/

WITH distinct_locations AS (
    SELECT DISTINCT
        city,
        state,
        country,
        is_remote
    FROM {{ ref('stg_jobs') }}
    WHERE city IS NOT NULL
),

location_stats AS (
    SELECT
        city,
        state,
        COUNT(*) AS job_count,
        AVG(salary_midpoint) AS avg_salary,
        SUM(CASE WHEN is_remote THEN 1 ELSE 0 END) AS remote_job_count
    FROM {{ ref('stg_jobs') }}
    GROUP BY city, state
),

dim_locations AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['dl.city', 'dl.state', 'dl.country']) }} AS location_key,
        
        -- Location attributes
        dl.city,
        dl.state,
        dl.country,
        
        -- Full location string
        CONCAT(
            COALESCE(dl.city, ''),
            CASE WHEN dl.city IS NOT NULL AND dl.state IS NOT NULL THEN ', ' ELSE '' END,
            COALESCE(dl.state, ''),
            CASE WHEN (dl.city IS NOT NULL OR dl.state IS NOT NULL) AND dl.country IS NOT NULL THEN ', ' ELSE '' END,
            COALESCE(dl.country, '')
        ) AS location_full,
        
        -- Region mapping (US-centric)
        CASE
            WHEN dl.state IN ('CA', 'OR', 'WA', 'AK', 'HI') THEN 'West'
            WHEN dl.state IN ('AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY') THEN 'Mountain'
            WHEN dl.state IN ('TX', 'OK', 'AR', 'LA') THEN 'South Central'
            WHEN dl.state IN ('AL', 'FL', 'GA', 'KY', 'MS', 'NC', 'SC', 'TN', 'VA', 'WV') THEN 'Southeast'
            WHEN dl.state IN ('IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'SD', 'WI') THEN 'Midwest'
            WHEN dl.state IN ('CT', 'DE', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT', 'DC') THEN 'Northeast'
            ELSE 'Other'
        END AS region,
        
        -- Remote-friendly indicator
        CASE WHEN dl.is_remote THEN TRUE ELSE FALSE END AS is_remote_friendly,
        
        -- Stats from jobs
        COALESCE(ls.job_count, 0) AS job_count,
        ls.avg_salary,
        COALESCE(ls.remote_job_count, 0) AS remote_job_count,
        
        -- Market tier based on job volume
        CASE
            WHEN ls.job_count >= 100 THEN 'Tier 1'
            WHEN ls.job_count >= 25 THEN 'Tier 2'
            WHEN ls.job_count >= 5 THEN 'Tier 3'
            ELSE 'Tier 4'
        END AS market_tier

    FROM distinct_locations dl
    LEFT JOIN location_stats ls 
        ON dl.city = ls.city AND dl.state = ls.state
)

SELECT * FROM dim_locations
