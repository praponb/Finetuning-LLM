SELECT
    id,
    quantity,
    unit_price,
    product_id
FROM order_items
WHERE quantity = (SELECT MIN(quantity) FROM order_items);
