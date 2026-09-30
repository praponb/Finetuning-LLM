SELECT
    id,
    sale_date,
    revenue,
    product_id
FROM store_sales
WHERE sale_date >= (SELECT MAX(sale_date) FROM store_sales WHERE revenue BETWEEN 2300 AND 2900)
ORDER BY product_id
