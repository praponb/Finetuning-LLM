select ss.id, ss.revenue, ss.units, p.brand, s.supplier_name from store_sales ss join products p on ss.product_id = p.id join suppliers s on p.supplier_id = s.id where p.supplier_id = 30
