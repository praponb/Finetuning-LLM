SELECT
    unit_price,
    product_id,
    GREATEST(unit_price, quantity) greatest_unit_price_quantity,
    COALESCE(quantity, 0) quantity_filled
FROM order_items
WHERE quantity BETWEEN 4 AND 7
