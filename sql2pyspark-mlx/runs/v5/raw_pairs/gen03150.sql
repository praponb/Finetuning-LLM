SELECT location, dept_name, budget, ROUND(budget * 1.15, 2) AS budget_scaled FROM departments WHERE budget >= 3144000
