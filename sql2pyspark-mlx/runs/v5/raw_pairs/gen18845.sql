SELECT location, MIN(budget) AS min_budget FROM departments GROUP BY location HAVING MIN(budget) >= 3704000 ORDER BY 2;
