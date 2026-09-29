SELECT
    id,
    store_id,
    product_id,
    units,
    revenue,
    ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id, store_id), 4) AS revenue_share,
    AVG(units) OVER (ORDER BY units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS units_avg_6,
    SUM(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
    ROUND(units / SUM(units) OVER (PARTITION BY product_id), 4) AS units_share,
    AVG(units) OVER (PARTITION BY product_id) AS product_id_avg_units
FROM store_sales
WHERE revenue BETWEEN 1900 AND 3300;
