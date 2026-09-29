SELECT
    id,
    dept_name,
    budget,
    SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
    NTILE(10) OVER (PARTITION BY location ORDER BY budget DESC, id) AS budget_bucket,
    LAG(budget, 2) OVER (PARTITION BY dept_name ORDER BY budget, id) AS prev_budget,
    SUM(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
FROM departments
