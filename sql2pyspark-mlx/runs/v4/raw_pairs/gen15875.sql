SELECT id, product_id, sale_date
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY sale_date DESC) AS row_num
    FROM store_sales
    WHERE product_id IN (2, 3)
) AS r
WHERE row_num <= 3
ORDER BY product_id DESC;
