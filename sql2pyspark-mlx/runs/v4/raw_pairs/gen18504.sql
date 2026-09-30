WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, TO_DATE(started_at) AS started_at_date
    FROM sessions
    WHERE started_at > DATE '2023-06-01'
      AND browser IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT s.*, p.country AS customer_country
    FROM valid s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.event_time
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY customer_id ORDER BY started_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        ROW_NUMBER() OVER (PARTITION BY customer_country ORDER BY started_at, id) AS row_seq,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        ROW_NUMBER() OVER (ORDER BY event_time DESC, id) AS event_seq
    FROM latest_match
)
SELECT
    id,
    customer_id,
    device,
    started_at,
    country,
    started_at_date,
    customer_country,
    event_time,
    row_seq,
    CASE WHEN device RLIKE '^(tablet|desktop)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS device_zone
FROM enriched
ORDER BY row_seq NULLS LAST;
