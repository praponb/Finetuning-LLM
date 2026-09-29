WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', browser, device) AS browser_device_key,
        DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label,
        COALESCE(country, 'NONE') AS country_clean
    FROM sessions
    WHERE browser IS NOT NULL
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM base
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY is_new_burst, id) AS event_seq,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY device ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM sessionized
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at_label,
        country_clean,
        prev_started_at,
        island_id,
        event_seq,
        AVG(island_id) OVER w_running AS island_id_avg_5,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS event_seq_2,
        SUM(island_id) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_island_id
    FROM scored
    WINDOW w_running AS (PARTITION BY device ORDER BY prev_started_at, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        browser,
        customer_id,
        device,
        island_id_avg_5
    FROM enriched
) src
PIVOT (
    AVG(island_id_avg_5) AS amount
    FOR device IN ('desktop', 'tablet')
)
