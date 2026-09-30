SELECT
    id,
    order_id,
    quantity,
    product_id,
    quantity - unit_price AS line_total
FROM order_items;
