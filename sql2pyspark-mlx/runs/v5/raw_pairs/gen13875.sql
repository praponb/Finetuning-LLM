SELECT c.age, c.city, a.customer_id FROM customers c LEFT JOIN accounts a ON c.id = a.customer_id WHERE c.credit_limit <= 5300;
