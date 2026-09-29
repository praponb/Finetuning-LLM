SELECT last_restock FROM inventory WHERE quantity > 104 UNION ALL SELECT opened_date FROM stores WHERE sqft >= 9276 ORDER BY last_restock;
