WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        MONTH(signup_date) AS signup_date_month,
        CONCAT(segment, ' ', city) AS segment_city
    FROM customers
    WHERE signup_date >= DATE '2023-07-15'
      AND NOT is_active
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.started_at,
        x.device
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.started_at, c.device
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC NULLS LAST) AS latest_rank
    FROM latest_match
    QUALIFY latest_rank = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY city ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY city ORDER BY started_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM deduped
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY city ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at >= s.started_at
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date BETWEEN '2024-11-01' AND '2025-02-01'
        )
)
SELECT
    id,
    email,
    country,
    segment,
    age,
    signup_date_month,
    segment_city,
    started_at,
    latest_rank
FROM eligible
