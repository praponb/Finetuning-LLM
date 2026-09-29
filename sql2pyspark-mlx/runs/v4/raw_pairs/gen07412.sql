select id, revenue, date_add(sale_date, 59) as sale_date_plus_59d from store_sales where product_id in (6, 8) order by revenue desc, id desc;
