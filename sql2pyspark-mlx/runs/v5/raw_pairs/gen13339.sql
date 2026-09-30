SELECT
    id,
    dept_name,
    LAG(budget, 1) OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS prev_budget,
    SUM(budget) OVER w_recent AS dept_name_sum_budget,
    LEAD(budget, 1) OVER w_recent_2 AS next_budget,
    LAG(budget, 1, 0) OVER w_recent_2 AS prev_budget_2,
    NTILE(10) OVER w_part AS budget_bucket
FROM departments
WINDOW w_recent AS (PARTITION BY dept_name), w_recent_2 AS (PARTITION BY dept_name ORDER BY budget, id), w_part AS (ORDER BY budget, id);
