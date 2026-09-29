SELECT id, location, dept_name, budget, ROUND(budget * 1.07, 2) AS budget_adjusted FROM departments WHERE budget BETWEEN 1417000 AND 1854000 ORDER BY location DESC;
