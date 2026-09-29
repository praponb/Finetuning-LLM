SELECT a.id, a.balance, a.customer_id, c.segment, c.credit_limit, c.is_active FROM accounts a JOIN customers c ON c.id = a.customer_id WHERE c.email LIKE '%@gmail.com' AND c.age >= 47;
