with source_data as (
    SELECT *
    FROM {{ source('raw','JOB_POSTING') }}
)

SELECT 
    job_id,
    title,
    company,
    location,
    salary,
    posted_date,
    employment_type,
    create_date
FROM source_data