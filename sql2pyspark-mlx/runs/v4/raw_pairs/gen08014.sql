SELECT id, hire_date, salary, DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_date_label FROM employees WHERE hire_date <= '2025-06-02' AND manager_id IN (15, 32, 35, 37) ORDER BY salary DESC
