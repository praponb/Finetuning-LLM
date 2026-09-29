SELECT
    ss.product_id,
    ss.sale_date,
    s.opened_date,
    s.store_name,
    p.cost
FROM store_sales ss
FULL OUTER JOIN stores s ON s.id = ss.store_id
JOIN products p ON ss.product_id = p.id
WHERE p.brand IN ('Umbrella', 'Initech') AND p.stock_qty <= 119;
