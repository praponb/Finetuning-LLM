SELECT
    customer_id,
    amount,
    channel,
    CEIL(shipping_fee) AS shipping_fee_whole
FROM orders
WHERE NOT (status IN ('COMPLETED', 'SHIPPED', 'PENDING') AND discount BETWEEN 0.11 AND 0.15 AND status IN ('COMPLETED', 'PENDING', 'CANCELLED'));
