WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount) OVER (PARTITION BY method ORDER BY paid_at, id) AS next_amount,
        LAG(amount) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) AS event_seq,
        CUME_DIST() OVER (ORDER BY amount DESC NULLS LAST) AS amount_cume_dist
    FROM payments
    WHERE method IN ('promptpay', 'card')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount_cume_dist DESC NULLS FIRST, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY method, status ORDER BY amount_cume_dist NULLS FIRST) AS rank_in_group_2
    FROM ranked
    QUALIFY rank_in_group_2 = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM latest
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        next_amount,
        prev_amount,
        event_seq,
        amount_cume_dist,
        rank_in_group,
        SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM sessionized
    INTERSECT
    SELECT id
    FROM payments
    WHERE status IN ('REFUNDED', 'FAILED', 'SUCCESS')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN kept_keys k ON s.id = k.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.amount,
        src.paid_at,
        src.status,
        src.next_amount,
        src.prev_amount,
        src.amount_cume_dist,
        src.rank_in_group,
        d.discount,
        d.shipping_fee
    FROM reconciled src
    LEFT JOIN orders d
        ON src.order_id = d.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    order_id,
    SUM(CASE WHEN status = 'FAILED' THEN prev_amount ELSE 0 END) AS failed_prev_amount,
    COUNT(*) AS num_rows
FROM with_dims
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', paid_at), order_id), (DATE_TRUNC('MONTH', paid_at)), ());
