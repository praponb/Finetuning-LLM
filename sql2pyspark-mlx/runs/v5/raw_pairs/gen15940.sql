WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        CAST(event_time AS DATE) AS event_time_day
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-10-14 13:00:00'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.event_type,
        src.event_time_day,
        b.country,
        b.device,
        b.browser
    FROM valid src
    INNER JOIN sessions b
        ON src.session_id = b.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time_day DESC, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY event_time_day DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY row_num DESC, id) AS seq_no_2,
        FIRST_VALUE(device, TRUE) OVER (PARTITION BY session_id ORDER BY event_time_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_device,
        LAST_VALUE(country) IGNORE NULLS OVER w_ordered AS last_known_country
    FROM latest
    WHERE session_id IN (20, 34)
    WINDOW w_ordered AS (PARTITION BY device ORDER BY event_time_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    device,
    COUNT_IF(event_type = 'purchase') AS purchase_events,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(CASE WHEN browser = 'Chrome' THEN 1 END) AS chrome_count,
    COUNT(*) AS cnt
FROM scored
GROUP BY device
