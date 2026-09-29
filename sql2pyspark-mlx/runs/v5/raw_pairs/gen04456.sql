WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(job_title)) AS job_title_title,
        CEIL(salary) AS salary_whole
    FROM employees
    WHERE salary < 139000
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.full_name,
        e.dept_id,
        e.manager_id,
        e.job_title,
        e.salary,
        e.level,
        e.salary_whole,
        p.email AS employee_email,
        p.hire_date AS employee_hire_date
    FROM filtered e
    LEFT JOIN employees p
        ON e.manager_id = p.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        manager_id,
        job_title,
        salary,
        salary_whole,
        employee_hire_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS dedup_rank
        FROM with_dims
    ) t
    WHERE dedup_rank = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT manager_id
    FROM deduped
    INTERSECT ALL
    SELECT manager_id
    FROM employees
    WHERE LOWER(TRIM(email)) = 'carol@company.co'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.manager_id = k.manager_id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, job_title
    FROM kept_rows
    EXCEPT DISTINCT
    SELECT id, job_title
    FROM employees
    WHERE NOT is_remote
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.job_title = k.job_title
)
-- Join a dimension and aggregate
SELECT
    p.job_title AS employee_job_title,
    p.level,
    COUNT(*) AS num_rows,
    MAX(src.salary) AS max_salary,
    MAX(src.salary_whole) AS max_salary_whole
FROM verified src
INNER JOIN employees p ON src.manager_id = p.id
GROUP BY GROUPING SETS ((p.job_title, p.level), (p.job_title), ())
