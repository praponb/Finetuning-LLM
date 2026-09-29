WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN device = 'tablet' THEN 1 ELSE 0 END is_tablet,
        INITCAP(TRIM(browser)) browser_title
    FROM sessions
    WHERE device IN ('desktop', 'tablet', 'mobile')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY is_tablet, id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.duration_sec < 370
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, row_num - is_tablet row_num_minus_is_tablet
    FROM eligible
    WHERE started_at > TO_DATE('2023-02-25')
      AND country IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.browser,
        a.is_tablet,
        p.credit_limit,
        p.city,
        p.email
    FROM cleaned a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
    WHERE p.country = 'US'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, browser
    FROM enriched
    INTERSECT
    SELECT customer_id, browser
    FROM sessions
    WHERE device <> 'tablet'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.browser = k.browser
),
eligible_sessions AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.page NOT IN ('/checkout', '/product')
        )
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            customer_id,
            browser,
            city,
            credit_limit
        FROM eligible_sessions
    ) src
    PIVOT (
        MAX(credit_limit)
        FOR city IN ('Bangkok' AS bangkok, 'Berlin' AS berlin)
    )
)
SELECT
    s.customer_id,
    s.browser,
    s.bangkok,
    (SELECT AVG(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM by_type s;
