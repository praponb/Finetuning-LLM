SELECT
    id,
    order_id,
    method,
    COALESCE(amount, 0) AS amount_filled,
    ROUND(amount, 1) AS amount_rounded
FROM payments
WHERE amount BETWEEN 1030 AND 1320 AND order_id = 12;
