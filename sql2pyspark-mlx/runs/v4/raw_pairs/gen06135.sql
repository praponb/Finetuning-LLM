WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS(',', event_type, page) AS event_type_page_key,
        FLOOR(duration_sec) AS duration_sec_whole
    FROM web_events
    WHERE session_id IN (1, 4, 29)
),
filtered_web_events AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(event_type) AS event_type_lower
    FROM filtered
    WHERE event_time >= TO_DATE('2024-03-26')
      AND page IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(duration_sec_whole) OVER (ORDER BY duration_sec_whole, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_whole_avg_5,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS seq_no
    FROM filtered_web_events
),
joined AS (
    -- Attach sessions attributes
    SELECT s.*, p.browser
    FROM enriched s
    LEFT JOIN sessions p
        ON s.session_id = p.id
)
-- Grouped metrics
SELECT
    session_id,
    COUNT_IF(page = '/product') AS product_events,
    ROUND(SUM(duration_sec_whole) / NULLIF(COUNT(CASE WHEN page = '/cart' THEN 1 END), 0), 2) AS duration_sec_whole_per_cart,
    COUNT(CASE WHEN event_type = 'search' THEN 1 END) AS search_count
FROM joined
GROUP BY session_id
ORDER BY product_events;
