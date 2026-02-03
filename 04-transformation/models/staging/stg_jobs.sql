{{
    config(
        materialized='view',
        tags=['staging', 'jobs']
    )
}}

/*
    Staging model for job postings
    Source: Silver layer jobs_cleaned parquet files
*/

WITH source AS (
    SELECT * FROM {{ source('silver', 'jobs_cleaned') }}
),

staged AS (
    SELECT
        -- Primary key
        job_id,
        
        -- Job details
        title,
        title_normalized,
        company,
        
        -- Location
        location,
        city,
        state,
        country,
        is_remote,
        
        -- Compensation
        salary_min,
        salary_max,
        salary_currency,
        
        -- Job attributes
        job_type,
        description,
        skills,
        skill_count,
        
        -- URLs
        url,
        
        -- Dates
        posted_date,
        scraped_at,
        
        -- Metadata
        source AS data_source,
        processing_date,
        processing_timestamp,
        
        -- Derived fields
        CASE 
            WHEN salary_min IS NOT NULL AND salary_max IS NOT NULL 
            THEN (salary_min + salary_max) / 2
            WHEN salary_min IS NOT NULL THEN salary_min
            WHEN salary_max IS NOT NULL THEN salary_max
            ELSE NULL
        END AS salary_midpoint,
        
        -- Row hash for change detection
        {{ dbt_utils.generate_surrogate_key([
            'job_id', 
            'title', 
            'company', 
            'salary_min', 
            'salary_max'
        ]) }} AS row_hash

    FROM source
    WHERE 
        job_id IS NOT NULL
        AND title IS NOT NULL
        AND company IS NOT NULL
)

SELECT * FROM staged
