SELECT
    id,
    store_id,
    sale_date,
    revenue,
    product_id,
    LAG(revenue, 1, 0) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS prev_revenue,
    LAG(revenue, 1) OVER (PARTITION BY store_id, product_id ORDER BY sale_date, id) AS prev_revenue_2
FROM store_sales
WHERE revenue <= 3800;
