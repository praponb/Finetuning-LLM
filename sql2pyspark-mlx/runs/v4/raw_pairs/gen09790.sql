select id, store_id, sale_date, revenue, lead(revenue, 1) over (partition by store_id order by sale_date, id) as next_revenue from store_sales where product_id in (1, 7, 9);
