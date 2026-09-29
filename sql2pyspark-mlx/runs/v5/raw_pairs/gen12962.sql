WITH with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.opened_date,
        top1.account_status
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.opened_date, c.status AS account_status
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date < s.opened_date
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(age / SUM(age) OVER (PARTITION BY account_status), 4) AS age_share,
        AVG(credit_limit) OVER (PARTITION BY country, segment) AS country_avg_credit_limit
    FROM kept
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date <= s.opened_date
        )
),
with_latest_customers AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.email,
        s.is_active,
        s.credit_limit,
        s.age,
        s.age_share,
        s.country_avg_credit_limit,
        top1.started_at
    FROM eligible s
    JOIN LATERAL (
            SELECT c.started_at
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.opened_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    email,
    age,
    age_share,
    country_avg_credit_limit,
    started_at
FROM with_latest_customers
ORDER BY country_avg_credit_limit
