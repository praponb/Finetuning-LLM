SELECT i.quantity, ss.store_id, ss.units FROM inventory i LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE ss.sale_date > '2024-06-01'
