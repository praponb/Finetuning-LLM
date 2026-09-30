select id, revenue, store_id, last_day(sale_date) as sale_date_month_end from store_sales where (units <= 12 and product_id = 10) or store_id in (2, 3, 5)
