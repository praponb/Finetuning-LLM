SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    FIRST_VALUE(method) IGNORE NULLS OVER (PARTITION BY order_id, status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_method,
    COUNT(amount) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) amount_count_7d,
    LAG(amount, 1, 0) OVER (PARTITION BY status ORDER BY paid_at, id) prev_amount
FROM payments;
