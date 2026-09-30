WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(duration_sec AS DOUBLE) AS duration_sec_str,
        INITCAP(TRIM(page)) AS page_title,
        COALESCE(page, 'NONE') AS page_filled
    FROM web_events
    WHERE page IS NOT NULL
      AND event_type IN ('click', 'page_view', 'search')
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.page,
        src.duration_sec,
        src.page_filled,
        p.device,
        p.country
    FROM cleaned src
    INNER JOIN sessions p
        ON src.session_id = p.id
    WHERE p.country = 'UK'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        duration_sec,
        page_filled,
        device,
        LENGTH(country) AS country_length,
        COALESCE(page, 'NONE') AS page_filled_2
    FROM enriched
    WHERE country <> 'JP'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(country_length) OVER w_recent AS next_country_length,
        LAG(country_length) OVER w_recent AS prev_country_length,
        LAG(duration_sec) OVER w_recent AS prev_duration_sec,
        ROW_NUMBER() OVER w_recent AS seq_no,
        LEAD(country_length, 1) OVER w_grp AS next_country_length_2
    FROM filtered
    WINDOW w_recent AS (PARTITION BY device ORDER BY duration_sec ASC, id ASC), w_grp AS (ORDER BY country_length ASC, id ASC)
)
SELECT
    id,
    duration_sec,
    page_filled,
    device,
    country_length,
    page_filled_2,
    next_country_length,
    prev_country_length,
    seq_no
FROM with_windows
