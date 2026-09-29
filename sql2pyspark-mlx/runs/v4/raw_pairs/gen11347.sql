SELECT signup_date AS event_date
FROM customers
UNION
SELECT last_restock AS event_date
FROM inventory
WHERE store_id = 5
