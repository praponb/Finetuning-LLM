WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(amount, 1) amount_rounded,
        CONCAT_WS('/', status, method) status_method_key
    FROM payments
    WHERE method IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM sessions_payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) = 1
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM latest
),
sessions_payments_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps_2
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM sessions_payments_2
    INTERSECT
    SELECT id, status
    FROM payments
    WHERE method NOT IN ('card', 'paypal', 'bank_transfer')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_payments_2 s
    INNER JOIN matched_keys k ON s.id = k.id AND s.status = k.status
),
stats AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
        (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
        MAX(paid_at) AS ended_at,
        SORT_ARRAY(COLLECT_SET(method)) AS method_list,
        COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count,
        (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes_2
    FROM reconciled
    GROUP BY DATE_TRUNC('QUARTER', paid_at)
    HAVING (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 > 0
)
SELECT
    quarter_start,
    span_minutes,
    span_minutes_2
FROM stats
ORDER BY span_minutes_2 DESC
