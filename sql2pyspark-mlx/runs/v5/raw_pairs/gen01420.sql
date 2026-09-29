WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN browser IN ('Firefox', 'Edge') THEN 'Tier 1' ELSE 'Other' END AS browser_group,
        CONCAT(browser, ' ', device) AS browser_device
    FROM sessions
    WHERE country IS NOT DISTINCT FROM 'UK'
      AND browser IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.country AS customer_country,
        p.signup_date,
        p.city
    FROM base src
    JOIN customers p
        ON src.customer_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, LAST_VALUE(city, TRUE) OVER w_hist AS last_known_city
    FROM enriched
    WHERE device IN ('tablet', 'desktop', 'mobile')
    WINDOW w_hist AS (PARTITION BY device ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.duration_sec
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.signup_date
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY signup_date DESC NULLS LAST, id) AS rank_in_group
    FROM with_last
    QUALIFY rank_in_group = 1
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        country,
        (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
        (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min_2,
        ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN device = 'desktop' THEN 1 END), 0), 2) AS duration_sec_per_desktop,
        GROUPING(country) AS grouping_level
    FROM deduped
    GROUP BY ROLLUP (country)
)
SELECT duration_sec_per_desktop
FROM summary
ORDER BY duration_sec_per_desktop NULLS LAST
