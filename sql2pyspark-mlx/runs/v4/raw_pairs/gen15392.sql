SELECT
    id,
    product_id,
    store_id,
    sale_date,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id) rn,
    SUM(revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_revenue
FROM store_sales
WHERE revenue < 1400
