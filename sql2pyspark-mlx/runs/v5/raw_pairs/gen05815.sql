SELECT *, FLOOR(quantity) quantity_whole
FROM order_items
ORDER BY product_id DESC, id ASC
LIMIT 1
