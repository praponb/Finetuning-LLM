WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(salary * 0.9, 2) AS salary_adjusted,
        COALESCE(level, 'DIRECT') AS resolved_level
    FROM employees
    WHERE hire_date > DATE '2023-07-08'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY salary, id) AS rn
        FROM cleaned
    ) t
    WHERE rn = 1
),
joined AS (
    -- Attach departments attributes
    SELECT
        e.*,
        d.budget,
        d.dept_name
    FROM ranked e
    INNER JOIN departments d
        ON e.dept_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY rn DESC, id) AS event_seq,
        SUM(salary) OVER (PARTITION BY dept_name, manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        LAST_VALUE(salary_adjusted, TRUE) OVER (PARTITION BY manager_id, dept_name ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_salary_adjusted,
        LAG(budget, 1) OVER (ORDER BY salary, id) AS prev_budget
    FROM joined
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(prev_budget) OVER (PARTITION BY job_title, level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_budget,
        RANK() OVER (PARTITION BY dept_name ORDER BY salary) AS salary_rank,
        LAG(budget, 1) OVER (PARTITION BY manager_id ORDER BY salary_adjusted, id) AS prev_budget_2
    FROM with_windows
    WHERE manager_id IN (21, 25, 28)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        dept_name,
        CASE WHEN prev_budget >= 3730000 THEN 'HIGH' ELSE 'LOW' END AS prev_budget_tier,
        COUNT_IF(dept_name = 'Engineering') AS engineering_events,
        COUNT(*) FILTER (WHERE level = 'L1') AS l1_rows,
        COUNT_IF(level = 'L2') AS l2_events
    FROM enriched
    GROUP BY ROLLUP (dept_name, CASE WHEN prev_budget >= 3730000 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    prev_budget_tier,
    engineering_events,
    l2_events
FROM stats
ORDER BY prev_budget_tier DESC, engineering_events;
