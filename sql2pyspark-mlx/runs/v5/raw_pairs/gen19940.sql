SELECT product_id, order_id
FROM order_items
WHERE unit_price BETWEEN 260 AND 290 AND product_id IN (23, 35)
