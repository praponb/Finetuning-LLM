WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(country, 1, 3) AS country_prefix
    FROM sessions
    WHERE TO_DATE(started_at) >= '2025-04-01'
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.country AS customer_country,
        d.name,
        d.email
    FROM filtered a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.web_event_page
    FROM joined s
    JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.event_type
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY event_type ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY event_type ORDER BY started_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_last
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        country_prefix,
        customer_country,
        name,
        email,
        web_event_page,
        event_type,
        is_new_burst,
        session_id,
        SUM(session_id) OVER (PARTITION BY web_event_page, customer_id ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS session_id_sum_7d,
        COUNT(session_id) OVER (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS session_id_count_30d,
        LAG(session_id, 1) OVER (PARTITION BY web_event_page ORDER BY started_at, id) AS prev_session_id,
        AVG(is_new_burst) OVER (PARTITION BY device ORDER BY is_new_burst, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS is_new_burst_avg_5_prior,
        LEAD(session_id, 1) OVER (PARTITION BY country ORDER BY session_id, id) AS next_session_id
    FROM islands
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN session_id_sum_7d >= 45 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_sum_7d_tier,
        CASE WHEN session_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
        ROUND(AVG(is_new_burst), 2) AS avg_is_new_burst,
        SUM(CASE WHEN customer_country = 'DE' THEN prev_session_id ELSE 0 END) AS de_prev_session_id,
        APPROX_PERCENTILE(next_session_id, ARRAY(0.25, 0.5, 0.75)) AS next_session_id_quartiles
    FROM scored
    GROUP BY 1, 2
    HAVING avg_is_new_burst < 0
)
SELECT session_id_sum_7d_tier, session_id_tier
FROM grouped
ORDER BY session_id_sum_7d_tier DESC;
