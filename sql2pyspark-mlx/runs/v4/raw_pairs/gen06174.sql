SELECT is_remote, MIN(salary) min_salary, MIN(hire_date) first_hire_date FROM employees WHERE job_title != 'Intern' GROUP BY is_remote ORDER BY first_hire_date, min_salary DESC, is_remote LIMIT 5;
