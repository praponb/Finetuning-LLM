WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(country) IGNORE NULLS OVER w_recent AS last_known_country,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM sessions
    WINDOW w_recent AS (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY device ORDER BY started_at ASC, id ASC)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY event_seq ASC NULLS LAST, id ASC) AS rank_in_group
        FROM enriched
    ) t
    WHERE rank_in_group = 1
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.city,
        d.signup_date
    FROM latest src
    JOIN customers d
        ON src.customer_id = d.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(event_seq) OVER (PARTITION BY customer_id ORDER BY rank_in_group ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_seq_avg_3,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY city, customer_id ORDER BY rank_in_group ASC, id ASC) AS row_seq_2
    FROM with_dims
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND TO_DATE(ch.event_time) < '2023-11-26'
        )
),
with_dims_sessions AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.country AS customer_country,
        b.city AS customer_city,
        b.signup_date AS customer_signup_date
    FROM eligible e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    last_known_country,
    rank_in_group,
    event_seq_avg_3,
    row_seq_2
FROM with_dims_sessions
ORDER BY rank_in_group DESC NULLS FIRST;
