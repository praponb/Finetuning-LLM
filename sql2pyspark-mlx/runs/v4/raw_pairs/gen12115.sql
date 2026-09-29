SELECT cost, 'shipment' source
FROM shipments
WHERE shipped_date <= '2023-05-01'
UNION ALL
SELECT unit_price cost, 'order_item' source
FROM order_items
WHERE unit_price IS NULL
ORDER BY cost DESC;
