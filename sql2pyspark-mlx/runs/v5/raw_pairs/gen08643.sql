SELECT
    *,
    SUM(amount) OVER w_part AS amount_sum_7d,
    SUM(amount) OVER (PARTITION BY status) AS status_sum_amount,
    COUNT(amount) OVER w_grp AS amount_count_90d,
    SUM(amount) OVER w_running AS cumulative_amount,
    AVG(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior
FROM payments
WINDOW w_part AS (PARTITION BY method, order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
