WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        level,
        AVG(salary) OVER (PARTITION BY dept_id, level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_salary,
        MAX(salary) OVER (PARTITION BY dept_id, level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_salary,
        MIN(salary) OVER (PARTITION BY dept_id, level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_salary
    FROM employees
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(level, TRUE) OVER (ORDER BY running_min_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        SUM(salary) OVER (PARTITION BY level, dept_id ORDER BY running_min_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM enriched
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        running_min_salary,
        ROUND(salary * 0.85, 2) AS salary_adjusted,
        COALESCE(level, 'N/A') AS level_filled
    FROM metrics
    WHERE level IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(running_min_salary) OVER w_grp AS running_min_running_min_salary,
        AVG(running_min_salary) OVER w_grp AS running_avg_running_min_salary,
        MAX(running_min_salary) OVER w_grp AS running_max_running_min_salary,
        SUM(running_min_salary) OVER w_grp AS running_sum_running_min_salary
    FROM cleaned
    WHERE full_name IS NULL
    WINDOW w_grp AS (PARTITION BY manager_id ORDER BY running_min_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Join a dimension and aggregate
SELECT
    d.dept_name,
    COUNT(*) AS rows_total,
    AVG(a.running_avg_running_min_salary) AS avg_running_avg_running_min_salary
FROM scored a
INNER JOIN departments d ON a.dept_id = d.id
GROUP BY d.dept_name
ORDER BY avg_running_avg_running_min_salary DESC
