WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        SUBSTRING(country, 1, 2) country_prefix
    FROM sessions
    WHERE country IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.device,
        a.country,
        a.country_prefix,
        p.signup_date
    FROM valid a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= '2025-08-01'
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, d.segment
    FROM eligible a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id IN (10, 15)
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        country_prefix,
        signup_date,
        segment,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date, id) seq_no
    FROM not_flagged
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.country_prefix,
        s.signup_date,
        x.page,
        x.event_type
    FROM enriched s
    JOIN LATERAL (
            SELECT c.page, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    COUNT_IF(page = '/search') AS search_events,
    COUNT(*) FILTER (WHERE country = 'SG') AS sg_rows,
    COUNT(DISTINCT page) AS distinct_pages,
    COUNT(*) FILTER (WHERE country = 'DE') AS de_rows,
    COUNT(DISTINCT event_type) AS distinct_event_types,
    COUNT(*) AS row_count
FROM with_last
GROUP BY week_start
HAVING COUNT_IF(page = '/search') > 1;
