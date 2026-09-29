SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    SUM(amount) OVER (PARTITION BY method) AS method_sum_amount,
    COUNT(amount) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d,
    FIRST_VALUE(amount, TRUE) OVER (PARTITION BY method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
    ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY amount, id) AS seq_no,
    RANK() OVER (PARTITION BY order_id, method ORDER BY amount) AS amount_rank
FROM payments;
