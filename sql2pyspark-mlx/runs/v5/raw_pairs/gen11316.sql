WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        COALESCE(amount, 0) amount_filled
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-04-26 04:00:00')
      AND method IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, order_id
    FROM filtered
    EXCEPT
    SELECT status, order_id
    FROM payments
    WHERE method IN ('bank_transfer', 'promptpay')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.status = k.status AND s.order_id = k.order_id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY status) status_sum_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 4) amount_share
    FROM kept_rows
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        paid_at,
        amount_share,
        CUME_DIST() OVER (PARTITION BY status ORDER BY amount DESC) amount_cume_dist,
        STDDEV_SAMP(amount_share) OVER (PARTITION BY status) status_stddev_samp_amount_share,
        SUM(status_sum_amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) status_sum_amount_sum_7d,
        AVG(amount) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_avg_90d
    FROM enriched
    WHERE status = 'REFUNDED'
),
report AS (
    SELECT
        amount,
        paid_at,
        amount_share,
        amount_cume_dist,
        status_stddev_samp_amount_share
    FROM metrics
)
SELECT
    amount,
    paid_at,
    amount_cume_dist,
    status_stddev_samp_amount_share
FROM report
ORDER BY paid_at DESC
