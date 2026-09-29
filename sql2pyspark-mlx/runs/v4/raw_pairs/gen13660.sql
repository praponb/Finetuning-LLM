select ss.id, ss.store_id, p.launch_date, p.product_name from store_sales ss left join products p on ss.product_id = p.id
