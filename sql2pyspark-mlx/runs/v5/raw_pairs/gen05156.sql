WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        amount,
        paid_at,
        INITCAP(TRIM(method)) AS method_title,
        COALESCE(method, 'N/A') AS method_clean
    FROM payments
    WHERE method IS DISTINCT FROM 'bank_transfer'
      AND order_id IS NOT NULL
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM cleaned
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        method,
        amount,
        paid_at,
        method_clean,
        SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
remaining_keys AS (
    -- Set difference on keys
    SELECT method
    FROM islands
    EXCEPT ALL
    SELECT method
    FROM payments
    WHERE amount BETWEEN 420 AND 1330
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    JOIN remaining_keys k ON s.method = k.method
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        method,
        amount,
        paid_at,
        LOWER(method) AS method_lower,
        CASE WHEN method IN ('cash', 'paypal') THEN 'Group A' ELSE 'Other' END AS method_group
    FROM kept_rows
    WHERE paid_at >= TO_DATE('2023-09-29')
      AND method IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        method,
        paid_at,
        method_lower,
        method_group,
        DENSE_RANK() OVER w_hist AS amount_dense_rank,
        MAX(amount) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_max_90d,
        COUNT(amount) OVER w_recent AS amount_count_7d
    FROM valid
    WINDOW w_hist AS (PARTITION BY method ORDER BY amount NULLS FIRST), w_recent AS (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY amount_dense_rank DESC NULLS LAST) AS amount_dense_rank_rank,
        CUME_DIST() OVER w_part AS amount_dense_rank_cume_dist,
        RANK() OVER (ORDER BY amount_count_7d NULLS FIRST) AS amount_count_7d_rank,
        ROUND(amount_max_90d / SUM(amount_max_90d) OVER (PARTITION BY method), 4) AS amount_max_90d_share
    FROM with_windows
    WINDOW w_part AS (PARTITION BY method ORDER BY amount_dense_rank DESC)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        WEEKOFYEAR(paid_at) AS paid_at_weekofyear,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM metrics
    WHERE method IS NOT NULL
)
SELECT
    paid_at,
    method_lower,
    amount_dense_rank,
    amount_dense_rank_rank,
    amount_dense_rank_cume_dist,
    amount_count_7d_rank,
    amount_max_90d_share,
    paid_at_weekofyear,
    method_clean
FROM base
ORDER BY paid_at_weekofyear;
