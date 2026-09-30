SELECT id, supplier_id, cost, RANK() OVER (PARTITION BY supplier_id ORDER BY cost DESC) AS rnk FROM products ORDER BY supplier_id DESC, id DESC
