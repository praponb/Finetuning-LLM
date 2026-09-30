WITH matched_keys AS (
    -- Set difference on keys
    SELECT id, country
    FROM sessions
    EXCEPT ALL
    SELECT id, country
    FROM sessions
    WHERE device <> 'tablet'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions s
    JOIN matched_keys k ON s.id = k.id AND s.country = k.country
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        browser,
        TRIM(country) AS clean_country,
        CAST(started_at AS DATE) AS started_at_day
    FROM reconciled
    WHERE browser IS DISTINCT FROM 'Firefox'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at_day
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.duration_sec BETWEEN 156 AND 391
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.browser,
        s.started_at_day,
        c.segment,
        c.city
    FROM not_flagged s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
with_dims_sessions AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.age,
        p.country,
        p.signup_date
    FROM with_dims e
    JOIN customers p
        ON e.customer_id = p.id
    WHERE MONTH(p.signup_date) IN (3, 4, 11)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, country
    FROM with_dims_sessions
    EXCEPT ALL
    SELECT id, country
    FROM sessions
    WHERE started_at >= '2023-03-20'
),
reconciled_sessions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims_sessions s
    JOIN remaining_keys k ON s.id = k.id AND s.country = k.country
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at_day,
    s.segment,
    s.city,
    s.age,
    s.country,
    s.signup_date,
    (SELECT AVG(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM reconciled_sessions s
ORDER BY age NULLS FIRST;
