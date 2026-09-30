WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        amount,
        status,
        LENGTH(status) AS status_length
    FROM payments
    WHERE method IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status,
        status_length - amount AS status_length_minus_amount
    FROM base
    WHERE status = 'SUCCESS'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY status_length_minus_amount DESC, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        status,
        SUM(status_length_minus_amount) OVER w_recent AS cumulative_status_length_minus_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY status_length_minus_amount, id) AS event_seq,
        LEAD(status_length_minus_amount, 2, 0) OVER w_grp AS next_status_length_minus_amount
    FROM ranked
    WHERE status = 'REFUNDED'
    WINDOW w_recent AS (ORDER BY status_length_minus_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY status_length_minus_amount, id)
),
ranked_payments AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY event_seq NULLS LAST, id) = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        SUM(CASE WHEN status = 'FAILED' THEN next_status_length_minus_amount ELSE 0 END) AS failed_next_status_length_minus_amount,
        COUNT(DISTINCT id) AS distinct_ids
    FROM ranked_payments
    GROUP BY status
)
SELECT
    status,
    failed_next_status_length_minus_amount,
    distinct_ids,
    CASE WHEN status RLIKE '^(REFUNDED|FAILED)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS status_zone
FROM stats
ORDER BY status_zone NULLS FIRST;
