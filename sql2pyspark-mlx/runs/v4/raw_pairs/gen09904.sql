SELECT id, age FROM customers WHERE segment IN ('Corporate', 'SMB', 'Consumer') UNION SELECT id, age FROM customers WHERE segment <> 'SMB';
