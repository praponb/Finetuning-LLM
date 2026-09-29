SELECT
    id,
    product_id,
    store_id,
    sale_date,
    revenue,
    ROUND(AVG(revenue) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) revenue_moving_avg_4
FROM store_sales
WHERE product_id IN (2, 5)
ORDER BY store_id, 4 DESC;
