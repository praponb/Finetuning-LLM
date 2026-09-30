SELECT i.id, i.store_id, i.quantity, i.last_restock FROM inventory i WHERE i.last_restock <= '2024-02-24' AND i.product_id NOT IN (SELECT id FROM products WHERE cost < 530) ORDER BY i.id LIMIT 50;
