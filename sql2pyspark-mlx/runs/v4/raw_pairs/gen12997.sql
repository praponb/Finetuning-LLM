SELECT last_restock event_date
FROM inventory
WHERE product_id = 8
UNION
SELECT opened_date event_date
FROM accounts
ORDER BY event_date;
