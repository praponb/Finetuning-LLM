SELECT
    *,
    LAST_VALUE(revenue) IGNORE NULLS OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue,
    LEAD(revenue, 2, 0) OVER (PARTITION BY store_id, product_id ORDER BY units, id) AS next_revenue
FROM store_sales;
