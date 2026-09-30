SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory WHERE store_id IN (3, 5) ORDER BY id DESC LIMIT 5
