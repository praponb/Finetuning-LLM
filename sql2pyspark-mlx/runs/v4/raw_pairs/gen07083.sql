SELECT
    p.id,
    p.price,
    p.stock_qty,
    p.launch_date,
    s.city,
    s.supplier_name,
    s.country,
    DATE_TRUNC('YEAR', p.launch_date) AS launch_date_period
FROM products p
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.rating <= 2.3 AND s.supplier_name IS NOT NULL;
