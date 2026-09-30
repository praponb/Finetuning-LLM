SELECT id, dept_name, location, budget, INITCAP(TRIM(location)) AS location_title, CEIL(budget) AS budget_whole FROM departments WHERE budget <= 2147000 AND budget BETWEEN 2668000 AND 3717000
