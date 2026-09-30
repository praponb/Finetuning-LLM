SELECT
    oi.id,
    oi.order_id,
    oi.unit_price,
    p.brand,
    p.product_name,
    s.country
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.launch_date BETWEEN '2023-04-01' AND '2024-03-01' AND oi.unit_price >= 150;
