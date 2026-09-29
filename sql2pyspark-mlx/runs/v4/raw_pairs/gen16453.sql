SELECT COUNT(DISTINCT revenue) AS unique_stores, ROUND(STDDEV(units), 2) AS stddev_units FROM store_sales ORDER BY unique_stores DESC;
