SELECT c.segment, SUM(CASE WHEN a.account_type = 'SAVINGS' THEN a.balance ELSE 0 END) AS savings_balance FROM accounts a INNER JOIN customers c ON a.customer_id = c.id GROUP BY c.segment
