SELECT product_id, unit_price, order_id
FROM order_items
WHERE quantity <> 4 AND unit_price IS NULL AND unit_price > 190
ORDER BY unit_price ASC
