SELECT
    *,
    NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget, id) AS budget_bucket,
    LEAD(budget, 1) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget
FROM departments
