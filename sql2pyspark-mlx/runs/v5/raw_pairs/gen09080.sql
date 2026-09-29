SELECT *, LENGTH(dept_name) AS dept_name_length, ROUND(budget * 0.85, 2) AS adjusted_budget
FROM departments;
