SELECT 'employee' AS source, hire_date AS event_date FROM employees UNION SELECT 'account' AS source, opened_date AS event_date FROM accounts;
