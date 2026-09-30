SELECT
    id,
    supplier_id,
    stock_qty,
    cost,
    INITCAP(TRIM(brand)) AS brand_title
FROM products
WHERE MONTH(launch_date) IN (4, 7, 8);
