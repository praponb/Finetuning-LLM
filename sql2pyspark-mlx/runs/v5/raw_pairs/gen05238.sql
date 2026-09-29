SELECT products.category, inventory.last_restock, inventory.store_id FROM products INNER JOIN inventory ON products.id = inventory.product_id ORDER BY inventory.last_restock ASC;
