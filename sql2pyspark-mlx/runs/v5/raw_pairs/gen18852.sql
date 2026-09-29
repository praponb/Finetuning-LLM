select round(stddev(revenue), 2) as stddev_revenue from store_sales where store_id in (4, 5, 6) and units <= 11 order by stddev_revenue desc
