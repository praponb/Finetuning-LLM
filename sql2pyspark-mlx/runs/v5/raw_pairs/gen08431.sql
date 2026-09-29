WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        SPLIT(LOWER(email), '@')[0] email_user,
        UPPER(TRIM(full_name)) full_name_normalized
    FROM employees
    WHERE job_title IS NOT NULL
      AND level IS NOT DISTINCT FROM 'L5'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, job_title
    FROM valid
    INTERSECT
    SELECT id, job_title
    FROM employees
    WHERE manager_id = 32
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.job_title = k.job_title
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(salary) OVER (PARTITION BY level) level_max_salary,
        RANK() OVER (PARTITION BY job_title ORDER BY salary) salary_rank,
        DENSE_RANK() OVER (ORDER BY salary DESC) salary_dense_rank,
        PERCENT_RANK() OVER (ORDER BY salary) salary_percent_rank
    FROM kept_rows
),
enriched_employees AS (
    -- Attach employees attributes
    SELECT e.*, b.email
    FROM enriched e
    INNER JOIN employees b
        ON e.manager_id = b.id
),
joined AS (
    -- Attach employees attributes
    SELECT
        src.*,
        e.level AS employee_level,
        e.salary AS employee_salary
    FROM enriched_employees src
    LEFT JOIN employees e
        ON src.manager_id = e.id
    WHERE e.is_remote = TRUE
)
SELECT
    s.full_name,
    s.manager_id,
    s.salary,
    s.is_remote,
    s.level,
    s.email_user,
    s.salary_rank,
    s.salary_dense_rank,
    s.salary_percent_rank,
    CASE WHEN full_name RLIKE '^(  Carol Diaz|Emma Brown)$' THEN 'OTHER_ZONE' WHEN full_name RLIKE '^(Alice Wong|bob smith)$' THEN 'GROUP_B' ELSE 'CORE' END AS full_name_zone,
    (SELECT MIN(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS min_employee_salary
FROM joined s
ORDER BY manager_id DESC NULLS LAST
