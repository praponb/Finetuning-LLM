WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(country) country_lower,
        COALESCE(browser, 'DIRECT') resolved_browser
    FROM sessions
    WHERE device IN ('tablet', 'desktop')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.device,
        a.started_at,
        a.country,
        a.country_lower,
        d.signup_date,
        d.age,
        d.country AS customer_country
    FROM filtered a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.event_time
    FROM with_dims s
    JOIN LATERAL (
            SELECT c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.signup_date,
        s.customer_country,
        top1.web_event_event_time
    FROM with_last s
    JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device,
        customer_country,
        ROW_NUMBER() OVER (PARTITION BY customer_country ORDER BY web_event_event_time DESC, id) event_seq,
        FIRST_VALUE(customer_country, TRUE) OVER (PARTITION BY customer_country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_customer_country,
        LAST_VALUE(customer_country, TRUE) OVER (PARTITION BY device ORDER BY web_event_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_customer_country
    FROM with_latest
),
filtered_sessions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        customer_country,
        event_seq,
        first_known_customer_country,
        CONCAT_WS('-', customer_country, device) customer_country_device_key
    FROM metrics
    WHERE event_seq IS NOT NULL
      AND customer_country IS DISTINCT FROM 'SG'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        device,
        event_seq,
        first_known_customer_country,
        customer_country_device_key,
        FIRST_VALUE(event_seq) IGNORE NULLS OVER (PARTITION BY device ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_event_seq,
        AVG(event_seq) OVER (ORDER BY event_seq, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) event_seq_avg_5_prior,
        AVG(event_seq) OVER (ORDER BY event_seq, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) event_seq_avg_5_prior_2,
        NTILE(4) OVER (PARTITION BY customer_country ORDER BY event_seq, id) event_seq_bucket,
        AVG(event_seq) OVER (PARTITION BY device ORDER BY event_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) event_seq_avg_5
    FROM filtered_sessions
)
-- Grouped metrics
SELECT
    device,
    COUNT(*) AS event_count,
    SIZE(COLLECT_SET(device)) AS num_devices,
    COUNT(DISTINCT device) AS distinct_devices,
    COUNT(DISTINCT id) AS distinct_ids,
    SIZE(COLLECT_SET(device)) AS num_devices_2
FROM scored
GROUP BY device;
