WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        CASE WHEN event_type = 'click' THEN 1 ELSE 0 END AS is_click,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM web_events
    WHERE page IS NOT NULL
      AND event_type = 'page_view'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(duration_sec, 2) OVER (ORDER BY event_time, id) AS next_duration_sec,
        COUNT(is_click) OVER (PARTITION BY page) AS page_count_is_click
    FROM filtered
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        p.country,
        p.browser,
        p.device
    FROM with_windows src
    LEFT JOIN sessions p
        ON src.session_id = p.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY next_duration_sec DESC NULLS FIRST, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(page_count_is_click, 1) OVER (PARTITION BY device, session_id ORDER BY next_duration_sec, id) AS next_page_count_is_click,
        MAX(duration_sec) OVER (PARTITION BY page) AS page_max_duration_sec
    FROM deduped
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM scored
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        page,
        event_time,
        is_click,
        browser,
        page_max_duration_sec,
        is_new_session,
        session_id_2,
        AVG(duration_sec) OVER (PARTITION BY country ORDER BY next_page_count_is_click, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY country ORDER BY next_page_count_is_click, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY country ORDER BY next_page_count_is_click, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_duration_sec,
        LEAD(session_id_2, 1) OVER (PARTITION BY browser ORDER BY is_new_session, id) AS next_session_id_2
    FROM sessionized
    WHERE next_page_count_is_click >= 11
)
SELECT
    id,
    session_id,
    page,
    event_time,
    is_click,
    browser,
    page_max_duration_sec,
    is_new_session,
    session_id_2,
    CASE WHEN browser RLIKE '^(Firefox|Chrome)$' THEN 'OTHER_ZONE' WHEN browser RLIKE '^(Safari|Edge)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS browser_zone
FROM enriched
ORDER BY session_id DESC;
