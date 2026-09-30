WITH latest_match AS (
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        top1.employee_level
    FROM departments s
    JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rank_in_group
        FROM latest_match
    ) x
    WHERE rank_in_group = 1
),
with_windows AS (
    SELECT
        id,
        dept_name,
        employee_level,
        LEAD(budget) OVER (PARTITION BY employee_level ORDER BY budget, id) AS next_budget,
        ROW_NUMBER() OVER (PARTITION BY employee_level ORDER BY budget, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY employee_level ORDER BY budget DESC) AS budget_dense_rank
    FROM ranked
),
ranked_departments AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY seq_no DESC, id) = 1
),
with_windows_departments AS (
    SELECT
        *,
        LAST_VALUE(dept_name, TRUE) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
        CUME_DIST() OVER (PARTITION BY employee_level ORDER BY budget_dense_rank DESC NULLS LAST) AS budget_dense_rank_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY next_budget DESC, id) AS row_seq
    FROM ranked_departments
    WHERE seq_no BETWEEN 5 AND 6
)
SELECT
    employee_level,
    COUNT(DISTINCT employee_level) AS distinct_employee_levels,
    COUNT(*) AS event_count,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN employee_level = 'L2' THEN 1 END), 0), 2) AS seq_no_per_l2
FROM with_windows_departments
GROUP BY 1
HAVING seq_no_per_l2 < 1000000
ORDER BY employee_level;
