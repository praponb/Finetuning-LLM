WITH cleaned AS (
    SELECT
        id,
        amount,
        paid_at,
        status,
        CASE WHEN amount >= 1490 THEN 'High' WHEN amount >= 730 THEN 'Medium' ELSE 'Low' END AS amount_band
    FROM payments
    WHERE order_id IS NOT NULL
),
flagged AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
metrics AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY status ORDER BY amount DESC NULLS FIRST) AS amount_rank,
        NTILE(10) OVER (PARTITION BY status ORDER BY amount, id) AS amount_bucket,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY new_group_flag) AS new_group_flag_percent_rank
    FROM islands
),
cleaned_payments AS (
    SELECT *, CASE WHEN amount_rank >= 7 THEN 'High' WHEN amount_rank >= 4 THEN 'Medium' ELSE 'Low' END AS amount_rank_band
    FROM metrics
    WHERE paid_at >= TIMESTAMP '2023-09-22 15:00:00'
)
SELECT
    id,
    session_id,
    amount_rank,
    amount_bucket,
    new_group_flag_percent_rank,
    amount_rank_band
FROM cleaned_payments
ORDER BY id NULLS LAST;
