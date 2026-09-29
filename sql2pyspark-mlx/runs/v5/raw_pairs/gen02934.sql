SELECT id, credit_limit, name, segment FROM customers WHERE credit_limit >= (SELECT MAX(credit_limit) FROM customers) ORDER BY id DESC, name DESC LIMIT 20;
