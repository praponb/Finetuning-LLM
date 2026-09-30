SELECT id, quantity
FROM order_items
WHERE (quantity < 7 AND product_id IN (6, 14, 26)) OR product_id IN (17, 23, 35)
