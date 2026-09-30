WITH cleaned AS (
    SELECT
        *,
        ROUND(duration_sec * 1.1, 2) AS adjusted_duration_sec,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM web_events
    WHERE session_id IN (7, 15, 22, 27)
      AND page IS NOT NULL
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM latest
),
sessions_web_events AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
scored AS (
    SELECT
        id,
        session_id,
        event_time,
        page_filled,
        rank_in_group,
        AVG(adjusted_duration_sec) OVER (PARTITION BY event_type ORDER BY session_no, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS adjusted_duration_sec_avg_6,
        LAG(duration_sec, 1) OVER w_grp AS prev_duration_sec
    FROM sessions_web_events
    WHERE new_group_flag != 0
    WINDOW w_grp AS (PARTITION BY page ORDER BY session_no, id)
),
joined AS (
    SELECT
        e.*,
        d.device,
        d.browser
    FROM scored e
    LEFT JOIN sessions d
        ON e.session_id = d.id
),
pivoted AS (
    SELECT *
    FROM (
        SELECT
            device,
            browser,
            prev_duration_sec
        FROM joined
    ) src
    PIVOT (
        AVG(prev_duration_sec) AS total,
        COUNT(1) AS item_count
        FOR browser IN ('Chrome', 'Safari', 'Firefox')
    )
)
SELECT
    device,
    Chrome_total,
    Chrome_item_count,
    Safari_total,
    Safari_item_count,
    Firefox_total,
    Firefox_item_count
FROM pivoted
