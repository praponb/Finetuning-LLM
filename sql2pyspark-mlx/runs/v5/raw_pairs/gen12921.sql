SELECT inventory.id, inventory.product_id, inventory.quantity FROM inventory WHERE inventory.store_id IN (SELECT id FROM stores WHERE opened_date BETWEEN '2023-06-01' AND '2024-11-15')
