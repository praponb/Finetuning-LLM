SELECT e.id, e.product_id, e.quantity, e.last_restock, p.category, p.launch_date FROM inventory e JOIN products p ON e.product_id = p.id
