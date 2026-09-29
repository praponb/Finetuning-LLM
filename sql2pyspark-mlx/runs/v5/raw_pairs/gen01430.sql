SELECT
    order_date,
    status,
    amount * shipping_fee AS amount_with_shipping,
    amount - discount AS amount_minus_discount
FROM orders
ORDER BY order_date DESC, amount_with_shipping DESC;
