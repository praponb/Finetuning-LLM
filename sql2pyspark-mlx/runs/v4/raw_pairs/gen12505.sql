WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(status) AS clean_status,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2024-04-01 18:00:00')
      AND order_id IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY paid_at, id) AS row_seq,
        LAST_VALUE(session_start_flag) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_start_flag,
        SUM(session_id) OVER w_running AS session_id_sum_7d,
        SUM(amount) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM sessionized
    WINDOW w_running AS (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.discount,
        b.order_date
    FROM metrics a
    LEFT JOIN orders b
        ON a.order_id = b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        status,
        method,
        row_seq
    FROM with_dims
) src
PIVOT (
    MAX(row_seq) AS amount
    FOR method IN ('card' AS card, 'promptpay' AS promptpay, 'cash' AS cash)
);
