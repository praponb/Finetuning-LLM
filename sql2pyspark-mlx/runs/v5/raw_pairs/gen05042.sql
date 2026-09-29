SELECT s.store_name, s.opened_date, ss.product_id, ss.sale_date FROM stores s LEFT JOIN store_sales ss ON ss.store_id = s.id
