SELECT id, credit_limit FROM customers WHERE credit_limit > (SELECT AVG(credit_limit) FROM customers);
