SELECT i.last_restock, i.store_id, p.stock_qty FROM inventory i INNER JOIN products p ON i.product_id = p.id WHERE p.launch_date > '2025-09-01' OR p.cost > 550 ORDER BY store_id DESC
