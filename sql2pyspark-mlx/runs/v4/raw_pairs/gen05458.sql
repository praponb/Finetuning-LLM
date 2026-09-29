SELECT
    channel,
    SUM(discount) AS total_discount,
    SUM(CASE WHEN status = 'COMPLETED' THEN shipping_fee ELSE 0 END) AS completed_shipping_fee,
    MIN(shipping_fee) AS min_shipping_fee
FROM orders
WHERE NOT (amount >= 480 AND (shipping_fee <= 15 OR status NOT IN ('CANCELLED', 'PENDING', 'COMPLETED')))
GROUP BY channel;
