SELECT name FROM customers EXCEPT SELECT full_name AS name FROM employees WHERE YEAR(hire_date) = 2024;
