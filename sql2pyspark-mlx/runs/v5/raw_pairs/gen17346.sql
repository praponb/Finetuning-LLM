SELECT c.signup_date, c.segment, a.account_type FROM customers c LEFT JOIN accounts a ON a.customer_id = c.id WHERE c.age <= 35 AND a.status = 'OPEN' ORDER BY a.account_type, c.segment
