WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(city) AS city_upper
    FROM customers
    WHERE segment IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date < s.signup_date
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(age, 2) OVER (ORDER BY credit_limit, id) AS prev_age,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country), 4) AS credit_limit_share
    FROM kept
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.plan = 'premium'
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        PERCENT_RANK() OVER (ORDER BY prev_age DESC NULLS FIRST) AS prev_age_percent_rank,
        CORR(credit_limit, age) OVER (PARTITION BY city, country) AS corr_credit_limit_age,
        CORR(age, credit_limit) OVER (PARTITION BY city) AS corr_age_credit_limit,
        NTILE(10) OVER (PARTITION BY country ORDER BY credit_limit_share, id) AS credit_limit_share_bucket
    FROM not_flagged
    WHERE credit_limit BETWEEN 9100 AND 13500
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.discount
    FROM metrics s
    JOIN LATERAL (
            SELECT c.discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    city,
    measure,
    metric_value
FROM (
    SELECT
        id,
        city,
        corr_age_credit_limit,
        prev_age_percent_rank
    FROM latest_match
) src
UNPIVOT (
    metric_value FOR measure IN (corr_age_credit_limit, prev_age_percent_rank)
) u
