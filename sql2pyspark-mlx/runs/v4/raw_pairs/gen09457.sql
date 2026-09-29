WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        hire_date,
        level,
        CASE WHEN salary >= 147000 THEN 'High' WHEN salary >= 42000 THEN 'Medium' ELSE 'Low' END AS salary_level
    FROM employees
    WHERE job_title IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        hire_date,
        level,
        salary_level,
        CONCAT(level, ', ', email) AS level_email,
        SUBSTRING(job_title, 1, 2) AS job_title_prefix,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM base
    WHERE hire_date >= DATE '2024-03-27'
      AND job_title IS NOT NULL
),
base_employees AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        hire_date,
        level,
        level_email,
        job_title_prefix,
        job_title_clean,
        REPLACE(email, ' ', '') AS email_slug,
        CASE WHEN level IN ('L1', 'L3') THEN 'Tier 1' ELSE 'Other' END AS level_group
    FROM cleaned
    WHERE hire_date >= DATE '2023-07-06'
      AND level IS NOT NULL
)
SELECT
    id,
    full_name,
    email,
    hire_date,
    level_email,
    job_title_clean,
    email_slug,
    CASE WHEN full_name RLIKE '^(Jack Taylor|Noah Park)$' THEN 'GROUP_A' WHEN full_name RLIKE '^(Alice Wong|bob smith)$' THEN 'EXTENDED' ELSE 'CORE' END AS full_name_zone
FROM base_employees
ORDER BY hire_date DESC NULLS FIRST;
