SELECT ss.id, ss.units, p.price, p.cost, s.rating FROM store_sales ss RIGHT JOIN products p ON ss.product_id = p.id LEFT JOIN suppliers s ON p.supplier_id = s.id;
