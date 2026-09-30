SELECT product_id, store_id, YEAR(sale_date) AS sale_year, ROUND(STDDEV(revenue), 2) AS stddev_revenue FROM store_sales GROUP BY product_id, store_id, YEAR(sale_date);
