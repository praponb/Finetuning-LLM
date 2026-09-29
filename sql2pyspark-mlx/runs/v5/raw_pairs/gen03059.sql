SELECT sale_date FROM store_sales UNION SELECT last_restock AS sale_date FROM inventory WHERE quantity != 190 ORDER BY sale_date;
