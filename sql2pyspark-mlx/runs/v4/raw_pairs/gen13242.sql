select ss.id, ss.store_id, ss.revenue, p.cost, p.product_name, s.country from store_sales ss inner join products p on ss.product_id = p.id inner join suppliers s on p.supplier_id = s.id
