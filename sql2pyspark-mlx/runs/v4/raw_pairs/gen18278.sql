SELECT *, quantity - unit_price line_total, COALESCE(quantity, 0) quantity_filled
FROM order_items
WHERE product_id = 26
