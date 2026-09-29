WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        REGEXP_REPLACE(full_name, ' ', '_') AS full_name_slug
    FROM employees
    WHERE job_title IS NOT NULL
      AND hire_date >= '2024-07-01'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY level, job_title ORDER BY hire_date, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY hire_date DESC, id) AS seq_no_2
    FROM cleaned
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.*,
        p.job_title AS employee_job_title,
        p.full_name
    FROM metrics a
    LEFT JOIN employees p
        ON a.manager_id = p.id
)
SELECT
    s.id,
    s.manager_id,
    s.is_remote,
    s.level,
    s.full_name_slug,
    s.seq_no,
    s.employee_job_title,
    s.full_name,
    (SELECT MIN(x.salary) FROM employees x WHERE x.manager_id = s.manager_id) AS min_employee_salary
FROM joined s
