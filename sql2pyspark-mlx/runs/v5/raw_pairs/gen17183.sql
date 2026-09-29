WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        country,
        DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label
    FROM sessions
    WHERE browser NOT IN ('Chrome', 'Edge', 'Firefox')
      AND started_at >= DATE '2023-05-19'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.country,
        p.city,
        p.name,
        p.age
    FROM cleaned e
    INNER JOIN customers p
        ON e.customer_id = p.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= '2025-09-13'
        )
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.country AS customer_country,
        c.credit_limit,
        c.age AS customer_age
    FROM not_flagged a
    LEFT JOIN customers c
        ON a.customer_id <=> c.id
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.web_event_event_time
    FROM joined s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.country,
    s.city,
    s.age,
    s.customer_country,
    s.credit_limit,
    s.customer_age,
    (SELECT AVG(o2.discount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS avg_order_discount
FROM latest_match s
ORDER BY customer_age DESC NULLS LAST, country;
