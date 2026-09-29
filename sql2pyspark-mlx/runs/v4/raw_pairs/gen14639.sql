SELECT products.launch_date, products.supplier_id, inventory.last_restock, inventory.quantity FROM products LEFT JOIN inventory ON products.id = inventory.product_id;
