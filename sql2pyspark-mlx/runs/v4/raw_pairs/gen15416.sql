SELECT
    id,
    store_id,
    sale_date,
    units,
    SUM(units) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units
FROM store_sales
ORDER BY running_units
