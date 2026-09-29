SELECT
    store_id,
    YEAR(sale_date) AS sale_year,
    COUNT(*) AS total_count,
    ROUND(STDDEV(units), 2) AS stddev_units
FROM store_sales
GROUP BY store_id, YEAR(sale_date)
