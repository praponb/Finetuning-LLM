SELECT
    id,
    location,
    budget,
    MAX(budget) OVER w_ordered AS running_max_budget,
    COUNT(budget) OVER w_ordered AS running_count_budget,
    MIN(budget) OVER w_ordered AS running_min_budget,
    LEAD(budget, 2, 0) OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS next_budget
FROM departments
WINDOW w_ordered AS (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
