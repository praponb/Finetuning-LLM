WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), started_at) days_since_started_at,
        COALESCE(country, 'UNKNOWN') country_filled
    FROM sessions
    WHERE browser IS NOT NULL
      AND started_at >= TIMESTAMP '2023-07-01 18:00:00'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.signup_date,
        c.credit_limit,
        c.name
    FROM sessions_sessions a
    INNER JOIN customers c
        ON a.customer_id = c.id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.started_at
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        island_id,
        name,
        LEAD(island_id, 1) OVER w_recent next_island_id,
        LEAD(island_id, 1) OVER w_running next_island_id_2,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY island_id, id) seq_no,
        AVG(days_since_started_at) OVER w_running_2 days_since_started_at_avg_5_prior,
        NTILE(4) OVER (ORDER BY is_new_session DESC, id) is_new_session_bucket
    FROM eligible
    WINDOW w_recent AS (PARTITION BY country ORDER BY started_at, id), w_running AS (PARTITION BY browser ORDER BY credit_limit, id), w_running_2 AS (PARTITION BY device ORDER BY days_since_started_at, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY days_since_started_at_avg_5_prior, id) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN next_island_id_2 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_island_id_2_tier,
    customer_id,
    AVG(is_new_session_bucket) FILTER (WHERE device = 'tablet') AS tablet_avg_is_new_session_bucket,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(island_id) AS total_island_id,
    COUNT(*) AS cnt,
    SUM(seq_no) AS total_seq_no,
    SUM(CASE WHEN device = 'desktop' THEN island_id ELSE 0 END) AS desktop_island_id
FROM deduped
GROUP BY ROLLUP (CASE WHEN next_island_id_2 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, customer_id)
HAVING tablet_avg_is_new_session_bucket > 0;
