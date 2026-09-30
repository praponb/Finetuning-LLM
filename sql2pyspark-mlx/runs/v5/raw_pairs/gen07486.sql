WITH valid AS (
    SELECT *, TO_DATE(event_time) event_time_date
    FROM web_events
    WHERE event_time > TO_TIMESTAMP('2023-08-29 07:00:00')
      AND event_type IN ('page_view', 'add_to_cart')
),
ranked AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec, id) = 1
),
filtered AS (
    SELECT
        *,
        CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END is_page_view,
        COALESCE(page, 'NONE') page_clean
    FROM ranked
    WHERE event_time >= TO_DATE('2023-05-28')
),
joined AS (
    SELECT s.*, d.country
    FROM filtered s
    LEFT JOIN sessions d
        ON s.session_id = d.id
    WHERE d.device = 'tablet'
),
scored AS (
    SELECT
        *,
        LEAD(is_page_view, 1) OVER (PARTITION BY event_type ORDER BY event_time, id) next_is_page_view,
        AVG(is_page_view) OVER (ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) is_page_view_avg_2
    FROM joined
),
grouped AS (
    SELECT
        CASE WHEN is_page_view >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_page_view_tier,
        MIN(next_is_page_view) AS min_next_is_page_view,
        MIN(event_time) AS first_seen,
        AVG(next_is_page_view) AS avg_next_is_page_view,
        COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count
    FROM scored
    GROUP BY is_page_view_tier
)
SELECT
    is_page_view_tier,
    first_seen,
    jp_count
FROM grouped
ORDER BY first_seen DESC NULLS FIRST;
