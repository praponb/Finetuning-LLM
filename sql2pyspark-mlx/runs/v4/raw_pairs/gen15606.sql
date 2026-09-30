SELECT
    i.product_id,
    s.store_name,
    s.opened_date,
    p.stock_qty,
    p.brand,
    SUBSTRING(p.product_name, 1, 2) AS product_name_prefix
FROM inventory i
JOIN stores s ON i.store_id = s.id
JOIN products p ON i.product_id = p.id
ORDER BY product_id DESC
