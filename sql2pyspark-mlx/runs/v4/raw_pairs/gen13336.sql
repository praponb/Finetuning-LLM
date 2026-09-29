SELECT id
FROM order_items
EXCEPT ALL
SELECT id
FROM order_items
WHERE unit_price <= 240;
