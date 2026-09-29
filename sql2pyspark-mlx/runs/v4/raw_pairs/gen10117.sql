SELECT
    *,
    LEAD(revenue, 1) OVER (PARTITION BY product_id, store_id ORDER BY units, id) AS next_revenue,
    LAST_VALUE(revenue, TRUE) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue,
    SUM(revenue) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
FROM store_sales;
