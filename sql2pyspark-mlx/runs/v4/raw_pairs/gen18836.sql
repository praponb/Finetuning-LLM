WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(amount, 1) amount_rounded
    FROM payments
    WHERE paid_at > DATE '2023-12-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount_rounded) OVER (PARTITION BY status, order_id) status_avg_amount_rounded,
        AVG(amount_rounded) OVER (PARTITION BY order_id, method ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) amount_rounded_avg_2,
        ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at, id) seq_no
    FROM filtered
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) amount_share,
        SUM(status_avg_amount_rounded) OVER (PARTITION BY method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_status_avg_amount_rounded,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY row_seq DESC, id) row_seq_2
    FROM enriched
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_avg_amount_rounded >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_avg_amount_rounded_tier,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    MIN(paid_at) AS first_paid_at,
    PERCENTILE_APPROX(amount_share, 0.95) AS amount_share_p95,
    MIN(amount_rounded_avg_2) AS min_amount_rounded_avg_2,
    COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count
FROM scored
GROUP BY status_avg_amount_rounded_tier
ORDER BY status_avg_amount_rounded_tier DESC
