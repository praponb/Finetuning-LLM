SELECT
    product_id,
    MONTH(sale_date) AS sale_month,
    ROUND(STDDEV(revenue), 2) AS stddev_revenue,
    SUM(units) AS total_units
FROM store_sales
WHERE store_id IN (3, 5, 6)
GROUP BY product_id, MONTH(sale_date)
HAVING COUNT(DISTINCT revenue) > 2;
