SELECT
    id,
    quantity,
    product_id,
    LEAST(unit_price, quantity) AS least_unit_price_quantity
FROM order_items
WHERE product_id IS NOT NULL AND quantity <= 4 AND product_id = 37;
