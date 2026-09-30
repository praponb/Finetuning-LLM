WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(full_name, 'unassigned') full_name_filled,
        COALESCE(job_title, 'N/A') resolved_job_title
    FROM employees
    WHERE level IS DISTINCT FROM 'L1'
      AND manager_id IN (1, 32, 37)
),
enriched AS (
    -- Attach departments attributes
    SELECT s.*, d.location
    FROM valid s
    LEFT JOIN departments d
        ON s.dept_id = d.id
    WHERE d.dept_name IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(salary) OVER (PARTITION BY location ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_salary,
        MIN(salary) OVER (PARTITION BY location ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_salary,
        AVG(salary) OVER (PARTITION BY location ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) salary_share
    FROM enriched
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS rn
        FROM metrics
    ) x
    WHERE rn = 1
),
enriched_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(running_avg_salary) OVER (PARTITION BY dept_id, level ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_running_avg_salary,
        LAST_VALUE(running_max_salary, TRUE) OVER (PARTITION BY manager_id ORDER BY running_avg_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_running_max_salary,
        RANK() OVER (ORDER BY salary_share DESC) salary_share_rank,
        ROW_NUMBER() OVER (PARTITION BY location, dept_id ORDER BY hire_date, id) seq_no,
        LAST_VALUE(running_min_salary, TRUE) OVER (PARTITION BY location, dept_id ORDER BY running_min_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_running_min_salary
    FROM ranked
),
enriched_employees_2 AS (
    -- Attach departments attributes
    SELECT
        e.*,
        b.budget,
        b.dept_name
    FROM enriched_employees e
    LEFT JOIN departments b
        ON e.dept_id = b.id
)
-- Multi-level aggregation
SELECT
    job_title,
    CASE WHEN running_min_salary >= 140000 THEN 'LARGE' ELSE 'SMALL' END AS running_min_salary_tier,
    SUM(running_avg_salary) FILTER (WHERE location = 'Chiang Mai') AS chiang_mai_total_running_avg_salary,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    COUNT_IF(job_title = 'Designer') AS designer_events,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    MAX(seq_no) FILTER (WHERE level = 'L5') AS l5_max_seq_no,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles
FROM enriched_employees_2
GROUP BY 1, 2
ORDER BY running_min_salary_tier DESC
