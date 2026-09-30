SELECT
    id,
    product_id,
    sale_date,
    revenue,
    LAG(revenue, 2) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_revenue
FROM store_sales
