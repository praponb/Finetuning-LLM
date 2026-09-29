SELECT
    id,
    amount,
    LEAST(shipping_fee, amount) AS least_shipping_fee_amount,
    ROUND(amount, 1) AS amount_rounded
FROM orders
WHERE status = 'SHIPPED'
ORDER BY amount_rounded, id
LIMIT 10;
