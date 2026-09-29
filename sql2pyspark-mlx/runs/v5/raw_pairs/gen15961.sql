SELECT id, location, budget, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS budget_row FROM departments ORDER BY id
