SELECT
    id,
    product_id,
    sale_date,
    units,
    store_id,
    revenue,
    ROUND(AVG(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) units_moving_avg_7,
    DENSE_RANK() OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC) revenue_dense_rank
FROM store_sales
WHERE product_id IN (2, 4, 9)
ORDER BY units_moving_avg_7 DESC;
