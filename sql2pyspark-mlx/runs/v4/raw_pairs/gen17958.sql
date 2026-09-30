WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('YEAR', txn_time) AS txn_time_period
    FROM transactions
    WHERE txn_time > TO_TIMESTAMP('2024-01-07 01:00:00')
),
joined AS (
    -- Attach accounts attributes
    SELECT src.*, a.opened_date
    FROM valid src
    LEFT JOIN accounts a
        ON src.account_id = a.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY txn_type, merchant ORDER BY opened_date DESC) AS rank_in_group
        FROM joined
    ) t
    WHERE rank_in_group = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY txn_type ORDER BY txn_time_period) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN amount >= 3300 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    COUNT_IF(merchant = 'Amazon') AS amazon_events,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    PERCENTILE_APPROX(amount, 0.5) AS amount_p50
FROM latest
GROUP BY CASE WHEN amount >= 3300 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY amount_tier NULLS FIRST;
