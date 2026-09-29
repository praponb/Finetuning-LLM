SELECT
    s.supplier_name,
    p.cost,
    p.brand,
    p.stock_qty,
    UPPER(s.city) AS city_upper
FROM suppliers s
INNER JOIN products p ON s.id = p.supplier_id
