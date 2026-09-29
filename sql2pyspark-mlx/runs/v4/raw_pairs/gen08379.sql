SELECT
    id,
    location,
    budget,
    RANK() OVER (PARTITION BY location ORDER BY budget) AS rnk,
    budget - LAG(budget, 1, 0) OVER (PARTITION BY location ORDER BY budget, id) AS budget_change
FROM departments
