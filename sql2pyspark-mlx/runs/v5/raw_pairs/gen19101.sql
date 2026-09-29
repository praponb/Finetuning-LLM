WITH valid AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        DATE_FORMAT(started_at, 'MMM yyyy') AS started_at_label
    FROM sessions
    WHERE started_at >= DATE '2023-04-26'
),
with_latest AS (
    SELECT s.*, latest.duration_sec
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    SELECT
        e.*,
        p.age,
        p.country AS customer_country,
        p.segment
    FROM with_latest e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
enriched_sessions AS (
    SELECT
        *,
        LAST_VALUE(customer_country, TRUE) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_customer_country,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) AS row_seq,
        SUM(age) OVER (ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age
    FROM enriched
)
SELECT
    dim.segment AS customer_segment,
    src.country AS session_country,
    COUNT(*) AS cnt,
    AVG(src.age) AS avg_age,
    GROUPING_ID() AS level_id
FROM enriched_sessions src
JOIN customers dim ON src.customer_id = dim.id
GROUP BY GROUPING SETS ((dim.segment, src.country), (dim.segment), ());
