{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

/*
    Skills dimension table
    Exploded skills from job postings with categories
*/

WITH exploded_skills AS (
    -- Unnest/explode skills array from jobs
    SELECT
        job_id,
        skill,
        salary_midpoint,
        posted_date
    FROM {{ ref('stg_jobs') }},
    LATERAL FLATTEN(input => skills) AS s(skill)
    WHERE skills IS NOT NULL
),

skill_stats AS (
    SELECT
        skill,
        COUNT(DISTINCT job_id) AS job_count,
        AVG(salary_midpoint) AS avg_salary,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_midpoint) AS median_salary,
        MIN(posted_date) AS first_seen,
        MAX(posted_date) AS last_seen
    FROM exploded_skills
    GROUP BY skill
),

skill_categories AS (
    -- Map skills to categories
    SELECT * FROM {{ ref('skill_categories') }}
),

dim_skills AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['ss.skill']) }} AS skill_key,
        
        -- Skill attributes
        ss.skill AS skill_name,
        UPPER(ss.skill) AS skill_name_upper,
        
        -- Category from seed or derived
        COALESCE(sc.skill_category, 
            CASE
                WHEN ss.skill IN ('python', 'sql', 'java', 'scala', 'go', 'rust', 'javascript', 'r') THEN 'Programming Language'
                WHEN ss.skill IN ('spark', 'hadoop', 'flink', 'kafka') THEN 'Big Data'
                WHEN ss.skill IN ('aws', 'azure', 'gcp', 'google cloud') THEN 'Cloud Platform'
                WHEN ss.skill IN ('postgresql', 'mysql', 'mongodb', 'redis', 'snowflake', 'bigquery', 'redshift') THEN 'Database'
                WHEN ss.skill IN ('docker', 'kubernetes', 'terraform') THEN 'DevOps'
                WHEN ss.skill IN ('airflow', 'dagster', 'prefect', 'dbt') THEN 'Data Engineering Tool'
                WHEN ss.skill IN ('tableau', 'power bi', 'looker') THEN 'Visualization'
                WHEN ss.skill IN ('machine learning', 'deep learning', 'tensorflow', 'pytorch') THEN 'ML/AI'
                ELSE 'Other'
            END
        ) AS skill_category,
        
        -- Statistics
        ss.job_count,
        ss.avg_salary,
        ss.median_salary,
        
        -- Temporal
        ss.first_seen,
        ss.last_seen,
        
        -- Derived metrics
        CASE
            WHEN ss.job_count >= 100 THEN 'Very High'
            WHEN ss.job_count >= 50 THEN 'High'
            WHEN ss.job_count >= 20 THEN 'Medium'
            WHEN ss.job_count >= 5 THEN 'Low'
            ELSE 'Emerging'
        END AS demand_level,
        
        -- Salary premium compared to overall average
        (ss.avg_salary - (SELECT AVG(salary_midpoint) FROM {{ ref('stg_jobs') }} WHERE salary_midpoint IS NOT NULL)) AS salary_premium

    FROM skill_stats ss
    LEFT JOIN skill_categories sc ON LOWER(ss.skill) = LOWER(sc.skill_name)
)

SELECT * FROM dim_skills
ORDER BY job_count DESC
