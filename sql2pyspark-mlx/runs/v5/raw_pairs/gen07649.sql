WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain
    FROM customers
    WHERE segment IS NOT NULL
      AND city = 'Bangkok'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at < s.signup_date
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit NULLS FIRST, id) AS latest_rank
        FROM not_flagged
    ) x
    WHERE latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(credit_limit) OVER (PARTITION BY city) AS city_min_credit_limit,
        NTILE(3) OVER (PARTITION BY city, segment ORDER BY credit_limit, id) AS credit_limit_bucket
    FROM deduped
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN credit_limit >= 14000 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    SIZE(COLLECT_SET(city)) AS num_cities,
    AVG(credit_limit_bucket) FILTER (WHERE segment = 'SMB') AS smb_avg_credit_limit_bucket,
    COUNT_IF(segment = 'Consumer') AS consumer_events
FROM enriched
GROUP BY credit_limit_tier, quarter_start
