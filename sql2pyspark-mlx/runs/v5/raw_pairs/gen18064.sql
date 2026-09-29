WITH joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.started_at,
        src.country,
        d.age,
        d.email,
        d.signup_date
    FROM sessions src
    INNER JOIN customers d
        ON src.customer_id = d.id
    WHERE d.age >= 63
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        started_at,
        country,
        age,
        email,
        signup_date,
        LAG(age) OVER w_running AS prev_age,
        LEAD(age) OVER w_running AS next_age,
        ROW_NUMBER() OVER w_running AS row_seq,
        FIRST_VALUE(age, TRUE) OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age
    FROM joined
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY signup_date, id)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.signup_date
        )
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(next_age AS DOUBLE) AS next_age_str,
        COALESCE(country, 'DIRECT') AS resolved_country
    FROM kept
    WHERE country IS NOT NULL
      AND country IS NOT DISTINCT FROM 'SG'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) AS prev_started_at
    FROM base
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    country,
    SIZE(COLLECT_SET(country)) AS num_countries,
    APPROX_PERCENTILE(is_new_burst, 0.9) AS is_new_burst_p90,
    MAX(row_seq) FILTER (WHERE country = 'DE') AS de_max_row_seq,
    AVG(row_seq) AS avg_row_seq
FROM sessionized
GROUP BY CUBE (DATE_TRUNC('MONTH', started_at), country)
