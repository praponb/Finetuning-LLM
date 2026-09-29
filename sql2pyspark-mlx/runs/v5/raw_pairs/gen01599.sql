select id, product_id, revenue * units as revenue_per_unit, date_format(sale_date, 'yyyy-MM-dd') as sale_date_label from store_sales where product_id in (6, 7) order by product_id, id desc;
