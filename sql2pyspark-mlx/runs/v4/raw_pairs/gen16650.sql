SELECT
    id,
    supplier_id,
    price,
    RANK() OVER (PARTITION BY supplier_id ORDER BY price DESC) AS rnk
FROM products
ORDER BY supplier_id DESC;
