SELECT order_date AS event_date FROM orders UNION ALL SELECT opened_date AS event_date FROM accounts WHERE account_type = 'SAVINGS'
