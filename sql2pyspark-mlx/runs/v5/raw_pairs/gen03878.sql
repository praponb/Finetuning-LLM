SELECT
    id,
    dept_name,
    budget,
    location,
    PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS budget_pct_rank,
    AVG(budget) OVER (PARTITION BY location) AS location_avg_budget
FROM departments
WHERE budget > 1150000;
