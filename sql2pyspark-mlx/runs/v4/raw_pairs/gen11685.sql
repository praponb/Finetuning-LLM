WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        UPPER(method) AS method_upper,
        TO_DATE(paid_at) AS paid_at_date
    FROM payments
    WHERE paid_at >= TIMESTAMP '2024-04-28 04:00:00'
      AND order_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        STDDEV_SAMP(amount) OVER (PARTITION BY order_id) AS order_id_stddev_samp_amount,
        LEAD(amount, 1) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS next_amount
    FROM valid
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS event_seq,
        LEAD(next_amount, 2, 0) OVER (PARTITION BY order_id ORDER BY next_amount, id) AS next_next_amount
    FROM metrics
),
enriched_payments AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        method_upper,
        paid_at_date,
        running_amount,
        order_id_stddev_samp_amount,
        next_amount,
        event_seq,
        next_next_amount,
        COUNT(running_amount) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS running_amount_count_30d,
        ROUND(next_next_amount / SUM(next_next_amount) OVER (PARTITION BY method), 4) AS next_next_amount_share
    FROM enriched
)
-- Ranking and gap features
SELECT
    *,
    NTILE(5) OVER (PARTITION BY status ORDER BY running_amount, id) AS running_amount_bucket,
    LEAD(running_amount, 2) OVER (ORDER BY running_amount_count_30d, id) AS next_running_amount,
    ROW_NUMBER() OVER (ORDER BY paid_at_date, id) AS event_seq_2
FROM enriched_payments;
