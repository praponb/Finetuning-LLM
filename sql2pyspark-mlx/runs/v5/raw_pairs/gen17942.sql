SELECT
    id,
    product_id,
    sale_date,
    revenue,
    ROUND(AVG(revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS revenue_moving_avg_4,
    revenue - LAG(revenue, 2) OVER (ORDER BY sale_date, id) AS revenue_change
FROM store_sales;
