SELECT
    *,
    STDDEV_SAMP(amount) OVER w_part AS order_id_stddev_samp_amount,
    FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY order_id, method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
    LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY status, method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
    SUM(amount) OVER w_recent AS amount_sum_7d
FROM payments
WINDOW w_part AS (PARTITION BY order_id), w_recent AS (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW);
