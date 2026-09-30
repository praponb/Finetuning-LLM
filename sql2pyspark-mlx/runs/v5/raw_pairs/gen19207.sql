SELECT p.cost, p.supplier_id, p.brand, ss.revenue FROM products p LEFT JOIN store_sales ss ON ss.product_id = p.id;
