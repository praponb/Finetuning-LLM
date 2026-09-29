SELECT i.quantity, ss.units, ss.revenue FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE YEAR(i.last_restock) >= 2025 OR ss.product_id = 9
