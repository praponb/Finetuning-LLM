SELECT id, quantity, last_restock, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory ORDER BY quantity DESC;
