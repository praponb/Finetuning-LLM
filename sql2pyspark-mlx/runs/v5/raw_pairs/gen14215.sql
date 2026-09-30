select ss.id, ss.sale_date, ss.store_id, p.cost, s.city from store_sales ss join products p on ss.product_id = p.id join suppliers s on p.supplier_id = s.id where p.price >= 1140;
