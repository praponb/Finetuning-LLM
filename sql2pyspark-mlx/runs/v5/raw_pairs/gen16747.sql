SELECT unit_price AS value
FROM order_items
INTERSECT
SELECT revenue AS value
FROM store_sales
