SELECT id, supplier_id, stock_qty, DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC) dense_rnk FROM products WHERE SIZE(tags) >= 1;
