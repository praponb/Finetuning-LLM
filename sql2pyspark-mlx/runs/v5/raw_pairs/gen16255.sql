select id, product_id, date_format(sale_date, 'yyyy-MM-dd') as sale_date_label from store_sales where revenue between 2300 and 3300 and product_id = 8 and units = 23 order by product_id desc
