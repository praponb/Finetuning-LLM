SELECT
    id,
    units,
    product_id,
    sale_date
FROM store_sales
WHERE units IS NOT NULL
ORDER BY product_id DESC;
