select ss.id, ss.revenue, s.opened_date, p.supplier_id from store_sales ss inner join stores s on ss.store_id = s.id left join products p on ss.product_id = p.id where ss.product_id = 6;
