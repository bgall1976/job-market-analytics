{{
    config(
        materialized='table',
        tags=['marts', 'aggregates']
    )
}}

/*
    Skill demand trends over time
    Track how skill popularity changes week over week
*/

WITH weekly_jobs AS (
    SELECT
        DATE_TRUNC('week', posted_date) AS week_start,
        job_id,
        skills,
        salary_midpoint,
        is_remote
    FROM {{ ref('fact_job_postings') }}
    WHERE posted_date IS NOT NULL
),

weekly_skill_demand AS (
    SELECT
        wj.week_start,
        sk.skill_name,
        COUNT(DISTINCT wj.job_id) AS job_count,
        AVG(wj.salary_midpoint) AS avg_salary,
        SUM(CASE WHEN wj.is_remote THEN 1 ELSE 0 END) AS remote_count
    FROM weekly_jobs wj,
    LATERAL FLATTEN(input => wj.skills) AS s(skill_name)
    JOIN {{ ref('dim_skills') }} sk ON LOWER(s.skill_name) = LOWER(sk.skill_name)
    GROUP BY wj.week_start, sk.skill_name
),

with_trends AS (
    SELECT
        week_start,
        skill_name,
        job_count,
        avg_salary,
        remote_count,
        
        -- Previous week metrics
        LAG(job_count, 1) OVER (
            PARTITION BY skill_name 
            ORDER BY week_start
        ) AS prev_week_jobs,
        
        LAG(avg_salary, 1) OVER (
            PARTITION BY skill_name 
            ORDER BY week_start
        ) AS prev_week_salary,
        
        -- 4-week rolling average
        AVG(job_count) OVER (
            PARTITION BY skill_name 
            ORDER BY week_start 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS rolling_4wk_avg_jobs,
        
        -- Cumulative sum
        SUM(job_count) OVER (
            PARTITION BY skill_name 
            ORDER BY week_start
        ) AS cumulative_jobs

    FROM weekly_skill_demand
),

agg_demand_trends AS (
    SELECT
        week_start,
        skill_name,
        job_count,
        avg_salary,
        remote_count,
        
        -- Week over week changes
        prev_week_jobs,
        job_count - COALESCE(prev_week_jobs, 0) AS wow_job_change,
        CASE 
            WHEN prev_week_jobs > 0 
            THEN ROUND(100.0 * (job_count - prev_week_jobs) / prev_week_jobs, 1)
            ELSE NULL
        END AS wow_job_pct_change,
        
        prev_week_salary,
        ROUND(avg_salary - COALESCE(prev_week_salary, 0), 0) AS wow_salary_change,
        
        -- Rolling metrics
        ROUND(rolling_4wk_avg_jobs, 1) AS rolling_4wk_avg_jobs,
        
        -- Trend indicator
        CASE
            WHEN job_count > COALESCE(rolling_4wk_avg_jobs, 0) * 1.2 THEN 'Surging'
            WHEN job_count > COALESCE(rolling_4wk_avg_jobs, 0) * 1.05 THEN 'Growing'
            WHEN job_count < COALESCE(rolling_4wk_avg_jobs, 0) * 0.8 THEN 'Declining'
            WHEN job_count < COALESCE(rolling_4wk_avg_jobs, 0) * 0.95 THEN 'Slowing'
            ELSE 'Stable'
        END AS trend_indicator,
        
        -- Cumulative
        cumulative_jobs,
        
        -- Remote percentage
        CASE 
            WHEN job_count > 0 
            THEN ROUND(100.0 * remote_count / job_count, 1)
            ELSE 0
        END AS remote_percentage,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS updated_at

    FROM with_trends
)

SELECT * FROM agg_demand_trends
ORDER BY week_start DESC, job_count DESC
