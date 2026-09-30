SELECT location, COUNT(budget) budget_count FROM departments GROUP BY location ORDER BY budget_count DESC;
