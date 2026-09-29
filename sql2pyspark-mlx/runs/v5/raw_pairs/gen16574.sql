SELECT 'order' AS kind, shipping_fee
FROM orders
UNION ALL
SELECT 'store_sale' AS kind, revenue
FROM store_sales
ORDER BY shipping_fee DESC;
