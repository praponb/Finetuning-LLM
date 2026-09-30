SELECT order_date event_date, 'order' source FROM orders UNION SELECT hire_date event_date, 'employee' source FROM employees WHERE is_remote;
