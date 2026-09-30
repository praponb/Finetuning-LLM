SELECT
    ss.id,
    ss.revenue,
    ss.store_id,
    p.price,
    p.stock_qty,
    s.supplier_name,
    s.city,
    DATE_SUB(ss.sale_date, 14) AS sale_date_minus_14d
FROM store_sales ss
FULL OUTER JOIN products p ON p.id = ss.product_id
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE s.rating <= 3.6
ORDER BY ss.store_id DESC, s.city;
