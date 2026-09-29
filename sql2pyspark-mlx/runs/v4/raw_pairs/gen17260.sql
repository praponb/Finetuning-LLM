SELECT products.price, inventory.product_id, inventory.quantity, inventory.last_restock FROM products LEFT JOIN inventory ON inventory.product_id = products.id;
