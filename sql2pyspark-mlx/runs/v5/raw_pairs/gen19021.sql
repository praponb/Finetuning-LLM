WITH deduped AS (
    SELECT
        id,
        amount,
        paid_at,
        status
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at NULLS FIRST, id) = 1
)
SELECT
    CASE WHEN amount >= 1630 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    MAX(amount) AS max_amount,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(amount) FILTER (WHERE status = 'REFUNDED') AS refunded_avg_amount,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    STDDEV_SAMP(amount) AS amount_stddev
FROM deduped
GROUP BY amount_tier, year_start
ORDER BY amount_stddev NULLS LAST
