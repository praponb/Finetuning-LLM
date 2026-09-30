WITH enriched AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        SUM(duration_sec) OVER w_grp AS running_duration_sec
    FROM web_events
    WINDOW w_grp AS (PARTITION BY session_id, page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    SELECT
        a.*,
        b.device,
        b.browser
    FROM enriched a
    LEFT JOIN sessions b
        ON a.session_id = b.id
),
enriched_web_events AS (
    SELECT
        s.*,
        p.country,
        p.browser AS session_browser,
        p.device AS session_device
    FROM joined s
    LEFT JOIN sessions p
        ON s.session_id = p.id
),
scored AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_bucket,
        running_duration_sec,
        device,
        browser,
        country,
        session_browser,
        session_device,
        AVG(running_duration_sec) OVER w_hist AS running_duration_sec_avg_5,
        COUNT(duration_sec) OVER (PARTITION BY session_device ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_count_30d
    FROM enriched_web_events
    WINDOW w_hist AS (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
with_dims AS (
    SELECT
        s.*,
        d.country AS session_country,
        d.device AS session_device_2
    FROM scored s
    INNER JOIN sessions d
        ON s.session_id = d.id
),
with_dims_web_events AS (
    SELECT
        s.*,
        p.country AS session_country_2,
        p.device AS session_device_3
    FROM with_dims s
    INNER JOIN sessions p
        ON s.session_id = p.id
),
joined_web_events AS (
    SELECT
        src.*,
        p.country AS session_country_3,
        p.device AS session_device_4,
        p.browser AS session_browser_2
    FROM with_dims_web_events src
    LEFT JOIN sessions p
        ON src.session_id = p.id
)
SELECT
    CASE WHEN running_duration_sec_avg_5 >= 1848 THEN 'LARGE' ELSE 'SMALL' END AS running_duration_sec_avg_5_tier,
    CASE WHEN duration_sec >= 488 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    COUNT(CASE WHEN session_device_3 = 'desktop' THEN 1 END) AS desktop_count,
    COUNT_IF(country = 'US') AS us_events,
    COUNT(*) FILTER (WHERE page = '/home') AS home_rows,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    BIT_AND(duration_sec) AS duration_sec_bit_and_2,
    COUNT(DISTINCT session_id) AS distinct_session_ids
FROM joined_web_events
GROUP BY CUBE (CASE WHEN running_duration_sec_avg_5 >= 1848 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN duration_sec >= 488 THEN 'HIGH' ELSE 'LOW' END);
