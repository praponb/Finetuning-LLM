SELECT
    *,
    LEAD(budget, 1) OVER (PARTITION BY location ORDER BY budget ASC, id ASC) AS next_budget,
    NTILE(3) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) AS budget_bucket,
    SUM(budget) OVER (PARTITION BY location ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
    DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST) AS budget_dense_rank
FROM departments;
