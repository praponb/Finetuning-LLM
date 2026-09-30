SELECT
    id,
    customer_id,
    order_date,
    channel,
    shipping_fee / NULLIF(discount, 0) AS shipping_fee_per_discount,
    amount / NULLIF(discount, 0) AS amount_per_discount
FROM orders
WHERE shipping_fee >= 39
ORDER BY customer_id
