WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN level = 'L3' THEN 1 ELSE 0 END AS is_l3,
        COALESCE(level, 'UNKNOWN') AS level_clean
    FROM employees
    WHERE hire_date > TO_DATE('2023-05-04')
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.*,
        p.email AS employee_email,
        p.job_title AS employee_job_title,
        p.level AS employee_level
    FROM base e
    INNER JOIN employees p
        ON e.manager_id = p.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY employee_level ORDER BY hire_date DESC, id) AS row_seq,
        LAST_VALUE(is_l3, TRUE) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_l3,
        FIRST_VALUE(job_title, TRUE) OVER w_ordered AS first_known_job_title
    FROM with_dims
    WINDOW w_ordered AS (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    dept_id,
    CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    employee_job_title,
    PERCENTILE_APPROX(last_known_is_l3, ARRAY(0.25, 0.5, 0.75)) AS last_known_is_l3_quartiles,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list_2
FROM with_windows
GROUP BY dept_id, CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END, employee_job_title
ORDER BY row_seq_tier
