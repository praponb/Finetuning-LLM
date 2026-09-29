SELECT budget, location, ROUND(budget, 2) AS budget_rounded
FROM departments
WHERE (budget BETWEEN 1251000 AND 3412000 AND budget < 3634000) OR location IN ('New York', 'Berlin');
