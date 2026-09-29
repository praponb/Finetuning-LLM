SELECT
    id,
    product_id,
    revenue,
    sale_date,
    units,
    RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS rnk,
    SUM(units) OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units
FROM store_sales
WHERE store_id IN (1, 4, 5, 6);
