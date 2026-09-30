WITH valid AS (
    SELECT *, CONCAT(page, ' ', event_type) AS page_event_type
    FROM web_events
    WHERE session_id IN (4, 20, 39)
),
scored AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        page_event_type,
        LEAD(duration_sec, 1) OVER (ORDER BY event_time, id) AS next_duration_sec,
        LAST_VALUE(page) IGNORE NULLS OVER (PARTITION BY event_type, session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_page
    FROM valid
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY page, event_type ORDER BY next_duration_sec DESC NULLS FIRST) AS row_num_2
    FROM latest
    QUALIFY row_num_2 = 1
),
joined AS (
    SELECT
        e.*,
        b.country,
        b.device
    FROM deduped e
    LEFT JOIN sessions b
        ON e.session_id = b.id
),
by_type AS (
    SELECT *
    FROM (
        SELECT
            country,
            page,
            next_duration_sec
        FROM joined
    ) src
    PIVOT (
        AVG(next_duration_sec)
        FOR page IN ('/checkout' AS checkout, '/cart' AS cart)
    )
)
SELECT country, cart
FROM by_type
ORDER BY country DESC NULLS LAST;
