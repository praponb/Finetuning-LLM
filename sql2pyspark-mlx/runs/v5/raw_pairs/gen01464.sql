select date_format(sale_date, 'yyyy-MM') as sale_month, count(revenue) as revenue_count from store_sales where product_id in (2, 3, 6, 9) and product_id = 2 group by sale_month order by sale_month;
