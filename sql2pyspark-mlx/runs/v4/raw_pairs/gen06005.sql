select round(stddev(revenue), 2) as stddev_revenue from store_sales where product_id in (3, 4) and month(sale_date) in (4, 7, 11);
