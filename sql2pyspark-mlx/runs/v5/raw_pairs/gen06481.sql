WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(amount, 1) AS amount_rounded
    FROM payments
    WHERE status IN ('FAILED', 'REFUNDED', 'SUCCESS')
      AND order_id IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        status,
        burst_id,
        ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) AS amount_share,
        COUNT(burst_id) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS burst_id_count_90d,
        PERCENT_RANK() OVER (PARTITION BY method ORDER BY amount DESC NULLS FIRST) AS amount_percent_rank
    FROM islands
    WHERE method = 'paypal'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        status,
        amount_share,
        burst_id_count_90d,
        amount_percent_rank,
        ROUND(amount_percent_rank * 1.1, 2) AS amount_percent_rank_scaled,
        CEIL(amount_percent_rank) AS amount_percent_rank_whole
    FROM with_windows
    WHERE method NOT IN ('bank_transfer', 'card')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY amount_percent_rank_whole DESC NULLS LAST) AS row_num
    FROM filtered
    QUALIFY row_num = 1
)
SELECT
    id,
    status,
    amount_share,
    amount_percent_rank,
    amount_percent_rank_scaled,
    row_num
FROM ranked;
