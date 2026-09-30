SELECT id, credit_limit, is_active, country FROM customers WHERE credit_limit > (SELECT AVG(credit_limit) FROM customers) ORDER BY id DESC
