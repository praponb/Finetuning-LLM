WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        segment,
        is_active,
        credit_limit,
        age,
        CONCAT(name, ' - ', city) AS name_city
    FROM customers
    WHERE city = 'New York'
      AND city IS DISTINCT FROM 'Bangkok'
),
cleaned_customers AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS('/', segment, name) AS segment_name_key
    FROM cleaned
    WHERE NOT is_active
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.credit_limit,
        s.name_city,
        x.started_at,
        x.session_browser
    FROM cleaned_customers s
    LEFT JOIN LATERAL (
            SELECT c.started_at, c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY credit_limit DESC, id) AS credit_limit_bucket,
        MIN(credit_limit) OVER w_grp AS session_browser_min_credit_limit,
        AVG(credit_limit) OVER w_grp AS session_browser_avg_credit_limit
    FROM latest_match
    WINDOW w_grp AS (PARTITION BY session_browser)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(session_browser_avg_credit_limit) OVER w_part AS session_browser_avg_credit_limit_max_90d,
        CUME_DIST() OVER (PARTITION BY session_browser ORDER BY session_browser_avg_credit_limit DESC NULLS FIRST) AS session_browser_avg_credit_limit_cume_dist,
        LAG(credit_limit, 1, 0) OVER w_part_2 AS prev_credit_limit,
        DENSE_RANK() OVER w_running AS credit_limit_dense_rank
    FROM scored
    WINDOW w_part AS (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY session_browser ORDER BY started_at, id), w_running AS (PARTITION BY session_browser ORDER BY credit_limit NULLS LAST)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    SUM(prev_credit_limit) AS total_prev_credit_limit,
    COUNT(*) FILTER (WHERE session_browser = 'Edge') AS edge_rows
FROM with_windows
GROUP BY DATE_TRUNC('WEEK', started_at)
ORDER BY week_start DESC NULLS FIRST;
