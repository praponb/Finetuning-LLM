SELECT location, SUM(budget) AS total_budget, ROUND(SUM(budget) * 100.0 / SUM(SUM(budget)) OVER (), 2) AS pct_of_total
FROM departments
WHERE budget < 2497000
GROUP BY location
ORDER BY total_budget, location;
