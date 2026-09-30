SELECT
    id,
    store_id,
    units,
    SUM(units) OVER (PARTITION BY store_id) AS store_id_sum_units
FROM store_sales
ORDER BY store_id DESC
