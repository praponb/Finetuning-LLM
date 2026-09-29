WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM islands
),
gaps_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst_2
    FROM with_prev
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps_2
),
matched_keys AS (
    -- Set difference on keys
    SELECT order_id, status
    FROM islands_payments
    EXCEPT
    SELECT order_id, status
    FROM payments
    WHERE amount BETWEEN 430 AND 1330
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands_payments s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.status = k.status
),
enriched AS (
    -- Attach orders attributes
    SELECT src.*, p.shipping_fee
    FROM reconciled src
    INNER JOIN orders p
        ON src.order_id = p.id
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.order_date,
        o.amount AS order_amount
    FROM enriched src
    INNER JOIN orders o
        ON src.order_id <=> o.id
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN amount >= 1710 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
        CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_burst_tier,
        method,
        APPROX_PERCENTILE(is_new_burst_2, 0.9) AS is_new_burst_2_p90,
        (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
        MIN(is_new_burst) AS min_is_new_burst,
        COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
        MAX(prev_paid_at) AS ended_at,
        COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count
    FROM enriched_payments
    GROUP BY CUBE (CASE WHEN amount >= 1710 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END, method)
)
SELECT
    amount_tier,
    is_new_burst_tier,
    is_new_burst_2_p90,
    duration_min,
    min_is_new_burst,
    ended_at
FROM summary
ORDER BY duration_min DESC;
