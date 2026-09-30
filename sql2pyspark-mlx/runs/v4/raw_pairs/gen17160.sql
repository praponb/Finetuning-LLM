SELECT budget, location, ROUND(budget, 1) AS budget_rounded FROM departments WHERE budget < 3685000 AND location <> 'Berlin' AND dept_name <> 'HR'
