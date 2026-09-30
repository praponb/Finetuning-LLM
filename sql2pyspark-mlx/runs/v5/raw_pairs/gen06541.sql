WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        TRIM(country) AS clean_country
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-06-22 02:00:00'
      AND browser IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        browser,
        country,
        CASE WHEN browser IN ('Edge', 'Firefox') THEN 'Priority' ELSE 'Other' END AS browser_group,
        TRIM(country) AS clean_country_2,
        COALESCE(country, 'UNKNOWN') AS country_filled
    FROM base
    WHERE browser IS NOT NULL
),
base_sessions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        browser,
        country,
        browser_group,
        country_filled,
        UPPER(country) AS country_upper
    FROM valid
    WHERE country IN ('DE', 'TH')
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.page,
        x.event_type
    FROM base_sessions s
    LEFT JOIN LATERAL (
            SELECT c.page, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    country,
    event_type,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list_2,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list
FROM latest_match
GROUP BY ROLLUP (country, event_type)
ORDER BY country DESC NULLS LAST;
