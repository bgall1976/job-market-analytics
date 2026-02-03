{{
    config(
        materialized='table',
        tags=['marts', 'aggregates']
    )
}}

/*
    Salary statistics aggregated by skill
    Key table for skill value analysis
*/

WITH skill_salaries AS (
    SELECT
        s.skill_name,
        s.skill_category,
        f.salary_min,
        f.salary_max,
        f.salary_midpoint,
        f.seniority_level,
        f.is_remote,
        l.region
    FROM {{ ref('fact_job_postings') }} f,
    LATERAL FLATTEN(input => f.skills) AS sk(skill_name)
    JOIN {{ ref('dim_skills') }} s ON LOWER(sk.skill_name) = LOWER(s.skill_name)
    LEFT JOIN {{ ref('dim_locations') }} l ON f.location_key = l.location_key
    WHERE f.salary_midpoint IS NOT NULL
),

agg_salary_by_skill AS (
    SELECT
        skill_name,
        skill_category,
        
        -- Volume metrics
        COUNT(*) AS job_count,
        
        -- Salary statistics
        ROUND(AVG(salary_midpoint), 0) AS avg_salary,
        ROUND(MIN(salary_midpoint), 0) AS min_salary,
        ROUND(MAX(salary_midpoint), 0) AS max_salary,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS p25_salary,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS median_salary,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS p75_salary,
        ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY salary_midpoint), 0) AS p90_salary,
        ROUND(STDDEV(salary_midpoint), 0) AS salary_stddev,
        
        -- Salary by seniority
        ROUND(AVG(CASE WHEN seniority_level = 'Junior' THEN salary_midpoint END), 0) AS avg_salary_junior,
        ROUND(AVG(CASE WHEN seniority_level = 'Mid-Level' THEN salary_midpoint END), 0) AS avg_salary_mid,
        ROUND(AVG(CASE WHEN seniority_level = 'Senior' THEN salary_midpoint END), 0) AS avg_salary_senior,
        
        -- Remote premium
        ROUND(AVG(CASE WHEN is_remote THEN salary_midpoint END), 0) AS avg_salary_remote,
        ROUND(AVG(CASE WHEN NOT is_remote THEN salary_midpoint END), 0) AS avg_salary_onsite,
        
        -- Regional breakdown
        ROUND(AVG(CASE WHEN region = 'West' THEN salary_midpoint END), 0) AS avg_salary_west,
        ROUND(AVG(CASE WHEN region = 'Northeast' THEN salary_midpoint END), 0) AS avg_salary_northeast,
        ROUND(AVG(CASE WHEN region = 'Midwest' THEN salary_midpoint END), 0) AS avg_salary_midwest,
        ROUND(AVG(CASE WHEN region = 'Southeast' THEN salary_midpoint END), 0) AS avg_salary_southeast,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS updated_at

    FROM skill_salaries
    GROUP BY skill_name, skill_category
    HAVING COUNT(*) >= 5  -- Minimum sample size
)

SELECT 
    *,
    -- Salary premium vs overall average
    avg_salary - (SELECT AVG(salary_midpoint) FROM {{ ref('fact_job_postings') }} WHERE salary_midpoint IS NOT NULL) AS salary_premium,
    -- Remote premium
    COALESCE(avg_salary_remote, 0) - COALESCE(avg_salary_onsite, 0) AS remote_premium
FROM agg_salary_by_skill
ORDER BY job_count DESC
