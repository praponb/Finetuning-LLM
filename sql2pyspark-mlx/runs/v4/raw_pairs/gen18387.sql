WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS FIRST, id) AS rank_in_group
        FROM web_events
    ) t
    WHERE rank_in_group = 1
),
joined AS (
    SELECT
        s.*,
        d.browser,
        d.device
    FROM ranked s
    INNER JOIN sessions d
        ON s.session_id = d.id
),
joined_web_events AS (
    SELECT
        s.*,
        b.country,
        b.browser AS session_browser,
        b.device AS session_device
    FROM joined s
    LEFT JOIN sessions b
        ON s.session_id = b.id
),
ranked_web_events AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_device, session_browser ORDER BY event_time DESC NULLS LAST, id) AS row_num
        FROM joined_web_events
    ) ranked
    WHERE row_num = 1
),
matched_keys AS (
    SELECT id
    FROM ranked_web_events
    EXCEPT ALL
    SELECT id
    FROM web_events
    WHERE page = '/cart'
),
kept_rows AS (
    SELECT s.*
    FROM ranked_web_events s
    INNER JOIN matched_keys k ON s.id = k.id
),
valid AS (
    SELECT *, CASE WHEN session_device IN ('mobile', 'desktop') THEN 'Priority' ELSE 'Other' END AS session_device_group
    FROM kept_rows
    WHERE event_time >= DATE '2024-02-15'
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_percent_rank,
        COUNT(duration_sec) OVER (PARTITION BY browser ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_count_30d
    FROM valid
)
SELECT *
FROM (
    SELECT
        session_browser,
        device,
        duration_sec_percent_rank
    FROM metrics
) src
PIVOT (
    AVG(duration_sec_percent_rank) AS value_sum,
    COUNT(1) AS cnt
    FOR device IN ('desktop', 'tablet')
)
