SELECT
    id,
    dept_name,
    budget,
    location,
    LEAD(budget, 1) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget,
    PERCENT_RANK() OVER (PARTITION BY dept_name, location ORDER BY budget DESC) AS budget_pct_rank
FROM departments
