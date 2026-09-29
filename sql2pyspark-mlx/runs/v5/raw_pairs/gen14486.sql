SELECT 'account' origin, opened_date event_date FROM accounts UNION SELECT 'employee' origin, hire_date event_date FROM employees;
