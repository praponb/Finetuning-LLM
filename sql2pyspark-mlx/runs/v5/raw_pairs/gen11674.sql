WITH filtered AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time,
        TO_DATE(event_time) AS event_time_date
    FROM web_events
    WHERE page = '/product'
),
with_windows AS (
    SELECT
        *,
        LAG(duration_sec) OVER w_grp AS prev_duration_sec,
        LAG(days_since_event_time) OVER w_grp AS prev_days_since_event_time,
        LEAD(days_since_event_time) OVER w_grp AS next_days_since_event_time,
        ROW_NUMBER() OVER w_grp AS row_seq,
        LAG(duration_sec, 2) OVER (PARTITION BY page ORDER BY days_since_event_time ASC, id ASC) AS prev_duration_sec_2
    FROM filtered
    WINDOW w_grp AS (PARTITION BY event_type ORDER BY duration_sec ASC, id ASC)
),
metrics AS (
    SELECT
        *,
        SUM(days_since_event_time) OVER w_ordered AS running_sum_days_since_event_time,
        MAX(days_since_event_time) OVER w_ordered AS running_max_days_since_event_time,
        COUNT(days_since_event_time) OVER w_ordered AS running_count_days_since_event_time
    FROM with_windows
    WHERE row_seq > 7
    WINDOW w_ordered AS (PARTITION BY event_type, page ORDER BY prev_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    SELECT src.*, b.browser
    FROM metrics src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
with_dims AS (
    SELECT
        s.*,
        b.browser AS session_browser,
        b.country
    FROM joined s
    LEFT JOIN sessions b
        ON s.session_id = b.id
)
SELECT
    CASE WHEN prev_duration_sec_2 >= 434 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_duration_sec_2_tier,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    MAX(event_time) AS last_seen,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    ROUND(SUM(running_count_days_since_event_time) / NULLIF(COUNT(CASE WHEN country = 'UK' THEN 1 END), 0), 2) AS running_count_days_since_event_time_per_uk,
    APPROX_PERCENTILE(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    MIN(next_days_since_event_time) AS min_next_days_since_event_time
FROM with_dims
GROUP BY GROUPING SETS ((CASE WHEN prev_duration_sec_2 >= 434 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', event_time)), (CASE WHEN prev_duration_sec_2 >= 434 THEN 'PREMIUM' ELSE 'STANDARD' END))
