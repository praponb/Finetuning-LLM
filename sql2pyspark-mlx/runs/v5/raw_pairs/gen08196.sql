WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser, device ORDER BY started_at DESC, id) AS rank_in_group
    FROM sessions
    QUALIFY rank_in_group = 1
),
not_flagged AS (
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND HOUR(x.event_time) BETWEEN 11 AND 14
        )
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rank_in_group DESC, id) AS row_num
    FROM not_flagged
    QUALIFY row_num = 1
),
with_windows AS (
    SELECT *, FIRST_VALUE(browser) IGNORE NULLS OVER w_part AS first_known_browser
    FROM deduped
    WINDOW w_part AS (PARTITION BY device ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match AS (
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.country,
        x.web_event_duration_sec,
        x.web_event_event_type
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec, c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    SELECT
        a.id,
        a.customer_id,
        a.country,
        a.web_event_duration_sec,
        a.web_event_event_type,
        b.country AS customer_country,
        b.age
    FROM latest_match a
    JOIN customers b
        ON a.customer_id = b.id
)
SELECT
    CASE WHEN age >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    country,
    SIZE(COLLECT_SET(country)) AS num_countries,
    ROUND(AVG(web_event_duration_sec), 2) AS avg_web_event_duration_sec,
    ROUND(AVG(age), 2) AS avg_age,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN age >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SIZE(COLLECT_SET(country)) DESC) AS num_countries_percent_rank
FROM enriched
GROUP BY age_tier, country
