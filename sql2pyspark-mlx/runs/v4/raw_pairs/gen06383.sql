SELECT a.id, a.account_type, c.credit_limit, c.is_active FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id WHERE NOT (a.customer_id IN (11, 14, 33, 35) AND a.opened_date < '2025-07-24')
