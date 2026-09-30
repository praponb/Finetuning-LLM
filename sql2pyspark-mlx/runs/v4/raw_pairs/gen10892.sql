SELECT id, city
FROM customers
WHERE segment IN ('Corporate', 'SMB', 'Consumer') OR is_active;
