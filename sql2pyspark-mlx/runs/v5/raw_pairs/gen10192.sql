WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(status) AS status_length,
        ROUND(amount, 1) AS amount_rounded
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-10-05 04:00:00'
      AND method = 'promptpay'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status_length,
        SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        o.shipping_fee,
        o.status
    FROM islands e
    LEFT JOIN orders o
        ON e.order_id = o.id
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.method,
        e.session_no,
        o.shipping_fee AS order_shipping_fee,
        o.order_date
    FROM enriched e
    LEFT JOIN orders o
        ON e.order_id = o.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT method, order_id
    FROM enriched_payments
    EXCEPT ALL
    SELECT method, order_id
    FROM payments
    WHERE amount <= 810
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_payments s
    INNER JOIN kept_keys k ON s.method = k.method AND s.order_id = k.order_id
)
-- Join a dimension and aggregate
SELECT
    d.channel,
    e.method,
    COUNT(*) AS rows_total,
    MAX(e.session_no) AS max_session_no,
    MIN(e.order_shipping_fee) AS min_order_shipping_fee
FROM kept_rows e
INNER JOIN orders d ON e.order_id = d.id
GROUP BY d.channel, e.method
ORDER BY max_session_no NULLS LAST;
