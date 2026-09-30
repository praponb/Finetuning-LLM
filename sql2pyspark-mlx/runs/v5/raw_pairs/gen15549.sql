select ss.id, ss.store_id, ss.sale_date from store_sales ss where ss.product_id not in (select id from products where supplier_id is not null)
