WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(browser) AS browser_lower
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND country IS NOT DISTINCT FROM 'JP'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device ORDER BY started_at DESC NULLS FIRST) AS row_num
        FROM filtered
    ) t
    WHERE row_num = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time <= s.started_at
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC NULLS FIRST, id) AS dedup_rank
        FROM kept
    ) x
    WHERE dedup_rank = 1
)
SELECT
    s.id,
    s.customer_id,
    s.started_at,
    s.country,
    s.browser_lower,
    s.row_num,
    s.dedup_rank,
    (SELECT SUM(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM latest s
ORDER BY id, started_at DESC
LIMIT 20;
