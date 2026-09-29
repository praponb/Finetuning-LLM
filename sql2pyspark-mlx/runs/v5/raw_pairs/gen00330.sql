SELECT
    store_id,
    MIN(sale_date) AS first_sale_date,
    ROUND(STDDEV(revenue), 2) AS stddev_revenue,
    MIN(revenue) AS min_revenue
FROM store_sales
GROUP BY store_id
HAVING ROUND(STDDEV(revenue), 2) < 80;
