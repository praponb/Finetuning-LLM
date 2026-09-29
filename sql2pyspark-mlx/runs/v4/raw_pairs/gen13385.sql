select id, revenue, product_id, units, date_trunc('year', sale_date) as sale_date_period from store_sales where store_id = 5 order by id desc limit 10
