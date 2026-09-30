WITH joined AS (
    SELECT
        e.*,
        p.location,
        p.budget
    FROM employees e
    LEFT JOIN departments p
        ON e.dept_id = p.id
),
scored AS (
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        level,
        location,
        LAG(budget, 2) OVER w_part AS prev_budget,
        AVG(salary) OVER (PARTITION BY job_title, location ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS salary_avg_6
    FROM joined
    WINDOW w_part AS (PARTITION BY manager_id ORDER BY budget, id)
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY location ORDER BY prev_budget DESC) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
metrics AS (
    SELECT
        id,
        dept_id,
        level,
        location,
        prev_budget,
        dedup_rank,
        FIRST_VALUE(level) IGNORE NULLS OVER w_running AS first_known_level,
        SUM(prev_budget) OVER w_part AS running_prev_budget,
        SUM(salary_avg_6) OVER w_hist AS running_salary_avg_6,
        LAG(salary_avg_6, 1) OVER w_hist_2 AS prev_salary_avg_6
    FROM ranked
    WINDOW w_running AS (PARTITION BY location ORDER BY salary_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (ORDER BY salary_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY job_title ORDER BY prev_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY level ORDER BY salary_avg_6, id)
),
with_dims AS (
    SELECT
        e.id,
        e.dept_id,
        e.location,
        e.dedup_rank,
        e.first_known_level,
        e.running_prev_budget,
        e.running_salary_avg_6,
        d.budget
    FROM metrics e
    LEFT JOIN departments d
        ON e.dept_id = d.id
)
SELECT
    p.location AS department_location,
    p.dept_name,
    COUNT(*) AS num_rows,
    MAX(s.running_prev_budget) AS max_running_prev_budget,
    GROUPING_ID() AS grouping_level
FROM with_dims s
LEFT JOIN departments p ON s.dept_id = p.id
GROUP BY ROLLUP (p.location, p.dept_name)
