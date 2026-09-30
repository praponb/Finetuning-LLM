SELECT unit_price AS value
FROM order_items
UNION
SELECT revenue AS value
FROM store_sales
ORDER BY value
