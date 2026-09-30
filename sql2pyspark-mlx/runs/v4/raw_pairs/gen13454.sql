SELECT id, age, country FROM customers WHERE segment NOT IN ('SMB', 'Consumer') UNION SELECT id, age, country FROM customers WHERE credit_limit BETWEEN 12200 AND 12300
