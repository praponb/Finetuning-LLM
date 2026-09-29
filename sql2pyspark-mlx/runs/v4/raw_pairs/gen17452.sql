SELECT
    id,
    product_id,
    units,
    sale_date
FROM store_sales
WHERE product_id IN (5, 8, 9, 10) AND product_id IN (1, 5, 8)
ORDER BY units, id
LIMIT 10;
