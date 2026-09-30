SELECT id, email, hire_date, dept_id, ROUND(salary * 1.1, 2) AS salary_scaled FROM employees WHERE (salary >= 85000 AND full_name LIKE 'F%') OR full_name LIKE 'Ol%';
