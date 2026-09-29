SELECT
    id,
    store_id,
    units,
    sale_date,
    units + revenue revenue_per_unit
FROM store_sales
WHERE store_id = 4 AND revenue <= 2300
ORDER BY store_id DESC, id
LIMIT 5;
