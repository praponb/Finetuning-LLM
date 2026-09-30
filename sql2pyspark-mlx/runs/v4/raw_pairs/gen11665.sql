WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        CAST(amount AS INT) AS amount_int,
        COALESCE(method, 'OTHER') AS method_filled
    FROM payments
    WHERE paid_at > TIMESTAMP '2024-01-15 18:00:00'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        amount_int,
        method_filled,
        ROW_NUMBER() OVER (ORDER BY amount_int, id) AS event_seq,
        ROW_NUMBER() OVER w_running AS seq_no,
        SUM(amount_int) OVER w_recent AS running_amount_int,
        AVG(amount_int) OVER w_part AS amount_int_avg_3
    FROM valid
    WINDOW w_running AS (PARTITION BY order_id ORDER BY amount_int DESC, id), w_recent AS (PARTITION BY method ORDER BY amount_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_windows
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    id,
    method,
    paid_at,
    status,
    amount_int,
    method_filled,
    event_seq,
    seq_no,
    running_amount_int
FROM islands
ORDER BY method
