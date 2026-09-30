SELECT last_restock event_date
FROM inventory
EXCEPT
SELECT opened_date event_date
FROM stores
