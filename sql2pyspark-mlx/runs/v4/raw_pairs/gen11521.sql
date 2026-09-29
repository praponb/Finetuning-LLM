SELECT
    ss.id,
    ss.product_id,
    ss.units,
    ss.sale_date,
    p.brand,
    p.price,
    p.cost
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
WHERE p.stock_qty BETWEEN 186 AND 369 OR p.brand IS NOT NULL
