WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date <= s.signup_date
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY credit_limit DESC, id) AS latest_rank
        FROM eligible
    ) t
    WHERE latest_rank = 1
),
deduped_customers AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        name,
        email,
        city,
        age
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY latest_rank DESC, id) = 1
),
with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.status,
        x.account_opened_date
    FROM deduped_customers s
    LEFT JOIN LATERAL (
            SELECT c.status, c.opened_date AS account_opened_date
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at < '2024-10-01'
        )
)
-- Aggregate with conditional metrics
SELECT
    status,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(age) AS avg_age,
    STDDEV_SAMP(age) AS age_stddev,
    COUNT_IF(status = 'FROZEN') AS frozen_events
FROM not_flagged
GROUP BY 1
HAVING distinct_ids > 1
ORDER BY distinct_ids DESC
