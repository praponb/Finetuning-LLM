SELECT
    id,
    product_id,
    units,
    sale_date,
    revenue,
    AVG(units) OVER (PARTITION BY product_id) AS product_id_avg_units,
    SUM(revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS running_revenue
FROM store_sales;
