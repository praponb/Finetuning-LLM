SELECT
    id,
    supplier_id,
    launch_date,
    NTILE(4) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS quartile
FROM products
WHERE UPPER(product_name) LIKE '%OGA%';
