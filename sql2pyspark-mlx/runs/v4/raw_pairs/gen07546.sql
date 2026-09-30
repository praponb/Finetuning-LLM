SELECT signup_date FROM customers WHERE is_active = TRUE UNION ALL SELECT last_restock FROM inventory WHERE product_id IN (2, 7, 8) ORDER BY signup_date;
