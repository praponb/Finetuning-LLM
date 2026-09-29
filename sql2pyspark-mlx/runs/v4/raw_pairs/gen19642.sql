WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(amount) OVER (PARTITION BY status) AS status_min_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS row_seq,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS amount_bucket,
        COUNT(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_count_7d
    FROM payments
)
-- Aggregate with conditional metrics
SELECT
    method,
    APPROX_PERCENTILE(amount_bucket, 0.9) AS amount_bucket_p90,
    PERCENTILE_APPROX(amount_count_7d, 0.95) AS amount_count_7d_p95,
    MAX(paid_at) AS ended_at
FROM scored
GROUP BY method
ORDER BY amount_bucket_p90;
