WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        QUARTER(hire_date) AS hire_date_quarter,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(job_title, 'UNKNOWN') AS resolved_job_title
    FROM employees
    WHERE hire_date > TO_DATE('2023-03-04')
      AND level IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(hire_date_quarter) OVER (PARTITION BY dept_id ORDER BY salary, id) AS prev_hire_date_quarter,
        LEAD(salary) OVER (PARTITION BY dept_id ORDER BY salary, id) AS next_salary,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) AS seq_no,
        SUM(hire_date_quarter) OVER (PARTITION BY job_title, level ORDER BY hire_date_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_hire_date_quarter
    FROM valid
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY seq_no DESC NULLS LAST, id) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT level, dept_id
    FROM latest
    EXCEPT
    SELECT level, dept_id
    FROM employees
    WHERE dept_id IN (3, 5, 11, 20)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.level = k.level AND s.dept_id = k.dept_id
)
SELECT
    id,
    full_name,
    salary,
    hire_date,
    is_remote,
    level,
    hire_date_quarter,
    is_remote_flag,
    resolved_job_title,
    CASE WHEN level RLIKE '^(L2|L4)$' THEN 'PRIORITY' WHEN level RLIKE '^(L1|L3)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS level_zone
FROM kept_rows
ORDER BY level
