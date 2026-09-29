SELECT c.id, c.credit_limit, c.age FROM customers c WHERE c.country NOT IN ('TH', 'JP', 'US') AND c.id IN (SELECT customer_id FROM accounts WHERE balance > 62000)
