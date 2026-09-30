select round(stddev(revenue), 2) as stddev_revenue, min(units) as min_units from store_sales where product_id in (1, 2, 5, 8) order by stddev_revenue desc
