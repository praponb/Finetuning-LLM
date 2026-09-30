SELECT id, segment
FROM customers
WHERE LENGTH(email) < 7;
