SELECT last_restock AS event_date, 'inventory' AS kind
FROM inventory
WHERE product_id IN (2, 3, 5, 6)
UNION
SELECT signup_date AS event_date, 'customer' AS kind
FROM customers
WHERE segment = 'Consumer'
ORDER BY event_date;
