SELECT
    id,
    sale_date,
    store_id,
    product_id,
    CEIL(units) AS units_whole
FROM store_sales
WHERE product_id = 8
