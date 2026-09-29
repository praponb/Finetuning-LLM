WITH filtered AS (
    SELECT
        *,
        TRIM(country) AS clean_country,
        CAST(started_at AS DATE) AS started_at_day,
        COALESCE(browser, 'N/A') AS browser_clean
    FROM sessions
    WHERE started_at >= TO_DATE('2023-07-08')
      AND country IS NOT NULL
),
with_gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
latest AS (
    SELECT *
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY session_id DESC, id) = 1
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY is_new_session DESC NULLS FIRST) AS is_new_session_percent_rank,
        SUM(is_new_session) OVER (PARTITION BY browser) AS browser_sum_is_new_session,
        SUM(session_id) OVER (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS session_id_sum_7d
    FROM latest
),
not_flagged AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at_day
        )
),
enriched AS (
    SELECT
        *,
        MAX(is_new_session_percent_rank) OVER (PARTITION BY device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_new_session_percent_rank_max_7d,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY session_id_sum_7d DESC, id) AS row_seq,
        STDDEV_SAMP(session_id_sum_7d) OVER (PARTITION BY browser) AS browser_stddev_samp_session_id_sum_7d
    FROM not_flagged
),
kept AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.session_id IN (8, 34)
        )
),
wide AS (
    SELECT *
    FROM (
        SELECT
            country,
            device,
            browser,
            browser_stddev_samp_session_id_sum_7d
        FROM kept
    ) src
    PIVOT (
        MAX(browser_stddev_samp_session_id_sum_7d) AS total
        FOR browser IN ('Chrome', 'Safari')
    )
)
SELECT
    country,
    device,
    Chrome,
    Safari,
    CASE WHEN device RLIKE '^(mobile|tablet)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS device_zone
FROM wide
ORDER BY Safari
