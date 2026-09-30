SELECT opened_date FROM stores UNION ALL SELECT last_restock FROM inventory WHERE product_id = 8 ORDER BY opened_date DESC;
