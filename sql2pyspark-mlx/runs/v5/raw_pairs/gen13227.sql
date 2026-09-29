WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        DATE_TRUNC('year', paid_at) AS paid_at_period,
        LENGTH(method) AS method_length
    FROM payments
    WHERE method = 'bank_transfer'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at_period) OVER (PARTITION BY order_id ORDER BY paid_at_period, id) IS NULL
                 OR paid_at_period > LAG(paid_at_period) OVER (PARTITION BY order_id ORDER BY paid_at_period, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        status,
        paid_at_period,
        method_length,
        SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
enriched AS (
    -- Attach orders attributes
    SELECT s.*, d.amount
    FROM islands s
    LEFT JOIN orders d
        ON s.order_id <=> d.id
    WHERE d.channel IN ('partner', 'store', 'mobile')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at_period - LAG(paid_at_period) OVER (PARTITION BY method ORDER BY paid_at_period, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM flagged
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(amount) OVER (PARTITION BY method ORDER BY method_length, id) AS next_amount,
        LAG(method_length) OVER (PARTITION BY method ORDER BY method_length, id) AS prev_method_length,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY method_length, id) AS row_seq
    FROM islands_payments
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        paid_at_period,
        method_length,
        amount,
        next_amount,
        prev_method_length,
        LAG(burst_id_2, 1) OVER w_running AS prev_burst_id_2,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY row_seq, id) AS event_seq
    FROM with_windows
    WINDOW w_running AS (PARTITION BY method ORDER BY burst_id_2, id)
)
SELECT
    id,
    paid_at_period,
    method_length,
    amount,
    next_amount,
    prev_method_length,
    prev_burst_id_2,
    event_seq
FROM metrics;
