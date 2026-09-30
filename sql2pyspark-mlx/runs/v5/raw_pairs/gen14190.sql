WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        COALESCE(city, 'N/A') AS resolved_city
    FROM customers
    WHERE segment IS DISTINCT FROM 'Consumer'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date <= s.signup_date
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        name,
        city,
        segment,
        signup_date,
        email_user,
        resolved_city
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY age DESC, id) AS latest_rank
        FROM eligible
    ) x
    WHERE latest_rank = 1
),
with_latest AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.session_device
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.device AS session_device
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.segment,
        s.email_user,
        s.resolved_city,
        x.account_type,
        x.account_status
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.account_type, c.status AS account_status
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    name,
    segment,
    resolved_city,
    account_type,
    account_status
FROM latest_match
ORDER BY id DESC NULLS FIRST
LIMIT 10;
