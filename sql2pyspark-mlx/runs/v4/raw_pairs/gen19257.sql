WITH base AS (
    SELECT *, LENGTH(event_type) event_type_length
    FROM web_events
    WHERE page IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY page, event_type ORDER BY event_time) AS rn
        FROM base
    ) x
    WHERE rn = 1
),
scored AS (
    SELECT
        *,
        LAG(event_type_length, 2) OVER (PARTITION BY event_type ORDER BY rn, id) prev_event_type_length,
        SUM(event_type_length) OVER (PARTITION BY event_type, session_id ORDER BY event_type_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_event_type_length,
        NTILE(4) OVER (PARTITION BY session_id ORDER BY event_type_length DESC, id) event_type_length_bucket
    FROM latest
)
SELECT
    CASE WHEN cumulative_event_type_length >= 407 THEN 'HIGH' ELSE 'LOW' END AS cumulative_event_type_length_tier,
    CASE WHEN prev_event_type_length >= 89 THEN 'HIGH' ELSE 'LOW' END AS prev_event_type_length_tier,
    MIN(cumulative_event_type_length) AS min_cumulative_event_type_length,
    COUNT(*) FILTER (WHERE page = '/cart') AS cart_rows
FROM scored
GROUP BY 1, 2;
