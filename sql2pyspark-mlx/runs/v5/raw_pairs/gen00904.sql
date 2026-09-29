WITH remaining_keys AS (
    SELECT id, page
    FROM web_events
    INTERSECT
    SELECT id, page
    FROM web_events
    WHERE session_id IN (7, 19, 31)
),
kept_rows AS (
    SELECT s.*
    FROM web_events s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.page = k.page
),
joined AS (
    SELECT
        a.id,
        a.session_id,
        a.page,
        a.duration_sec,
        p.device,
        p.browser,
        p.country
    FROM kept_rows a
    LEFT JOIN sessions p
        ON a.session_id = p.id
    WHERE HOUR(p.started_at) BETWEEN 11 AND 18
),
cleaned AS (
    SELECT
        *,
        ROUND(duration_sec, 1) AS duration_sec_rounded,
        COALESCE(device, 'NONE') AS device_filled
    FROM joined
    WHERE page IN ('/home', '/cart')
),
base AS (
    SELECT
        id,
        page,
        duration_sec,
        device,
        duration_sec_rounded,
        device_filled,
        COALESCE(country, 'unassigned') AS country_filled,
        LENGTH(page) AS page_length
    FROM cleaned
    WHERE country IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        duration_sec,
        device_filled,
        country_filled,
        page_length,
        RANK() OVER (PARTITION BY page, device ORDER BY duration_sec_rounded DESC NULLS LAST) AS duration_sec_rounded_rank,
        DENSE_RANK() OVER w_running AS duration_sec_dense_rank,
        DENSE_RANK() OVER w_part AS duration_sec_dense_rank_2
    FROM base
    WINDOW w_running AS (PARTITION BY page, device ORDER BY duration_sec NULLS LAST), w_part AS (PARTITION BY device ORDER BY duration_sec DESC)
)
SELECT
    CASE WHEN duration_sec >= 425 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(duration_sec_rounded_rank, 0.95) AS duration_sec_rounded_rank_p95,
    MAX(duration_sec_dense_rank) AS max_duration_sec_dense_rank
FROM with_windows
GROUP BY CASE WHEN duration_sec >= 425 THEN 'HIGH' ELSE 'LOW' END
ORDER BY duration_sec_rounded_rank_p95 DESC;
