WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount, 2) OVER (PARTITION BY method ORDER BY paid_at, id) AS next_amount,
        AVG(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id, status), 4) AS amount_share,
        DENSE_RANK() OVER (ORDER BY amount NULLS LAST) AS amount_dense_rank
    FROM payments
    WHERE method != 'cash'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount_dense_rank, id) AS rn
        FROM enriched
    ) x
    WHERE rn = 1
),
ranked_payments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS rank_in_group
    FROM ranked
    QUALIFY rank_in_group = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM ranked_payments
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        amount_avg_3,
        amount_dense_rank,
        rn,
        rank_in_group,
        SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions_payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, order_id
    FROM islands
    EXCEPT DISTINCT
    SELECT status, order_id
    FROM payments
    WHERE amount BETWEEN 410 AND 460
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN kept_keys k ON s.status = k.status AND s.order_id = k.order_id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.shipping_fee,
        d.channel
    FROM kept_rows e
    LEFT JOIN orders d
        ON e.order_id <=> d.id
)
-- Multi-level aggregation
SELECT
    order_id,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    AVG(amount_avg_3) AS avg_amount_avg_3,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END), 0), 2) AS session_no_per_success,
    MIN(amount_dense_rank) AS min_amount_dense_rank
FROM with_dims
GROUP BY order_id, week_start
HAVING (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 < 1000000;
