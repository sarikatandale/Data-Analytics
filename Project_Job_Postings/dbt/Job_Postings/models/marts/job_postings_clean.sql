{{ config(materialized='view') }}

SELECT
    job_id,
    title,
    company,
    location,
    salary,
    posted_date,
    employment_type,
    create_date
FROM {{ source('raw', 'JOB_POSTING') }}
WHERE title IS NOT NULL
    