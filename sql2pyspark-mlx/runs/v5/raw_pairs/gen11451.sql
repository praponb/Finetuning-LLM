SELECT job_title, DATE_SUB(hire_date, 51) AS hire_date_minus_51d FROM employees WHERE UPPER(email) LIKE '%ENR%' OR full_name NOT LIKE 'Iv%';
