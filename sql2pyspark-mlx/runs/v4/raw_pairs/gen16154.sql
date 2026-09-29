SELECT quantity, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory WHERE product_id = 6;
