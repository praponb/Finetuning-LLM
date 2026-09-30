WITH remaining_keys AS (
    -- Set difference on keys
    SELECT country
    FROM sessions
    EXCEPT
    SELECT country
    FROM sessions
    WHERE country = 'DE'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions s
    INNER JOIN remaining_keys k ON s.country = k.country
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.city,
        d.signup_date,
        d.country AS customer_country
    FROM not_flagged src
    INNER JOIN customers d
        ON src.customer_id = d.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.name,
        p.signup_date AS customer_signup_date,
        p.email
    FROM with_dims a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_type IN ('page_view', 'purchase', 'add_to_cart')
        )
),
eligible_sessions AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.signup_date
        )
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', customer_signup_date) AS week_start,
        (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
        COUNT_IF(customer_country = 'SG') AS sg_events,
        COUNT(DISTINCT city) AS distinct_cities
    FROM eligible_sessions
    GROUP BY DATE_TRUNC('WEEK', customer_signup_date)
)
SELECT
    week_start,
    duration_min,
    sg_events,
    distinct_cities
FROM totals
ORDER BY duration_min DESC NULLS FIRST
