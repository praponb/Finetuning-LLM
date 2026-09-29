SELECT hire_date FROM employees UNION SELECT start_date FROM subscriptions WHERE status = 'EXPIRED' ORDER BY hire_date DESC;
