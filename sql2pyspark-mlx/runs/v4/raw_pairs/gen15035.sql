SELECT
    id,
    location,
    budget,
    dept_name,
    ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS budget_row,
    COUNT(*) OVER (PARTITION BY dept_name) AS dept_name_count
FROM departments
WHERE budget <= 2183000
