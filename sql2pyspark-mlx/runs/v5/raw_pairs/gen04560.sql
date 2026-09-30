SELECT
    id,
    store_id,
    sale_date,
    units,
    revenue,
    units - LAG(units, 2) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS units_change,
    SUM(revenue) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue
FROM store_sales;
