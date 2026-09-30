SELECT
    *,
    ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) budget_share,
    RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) budget_rank
FROM departments
