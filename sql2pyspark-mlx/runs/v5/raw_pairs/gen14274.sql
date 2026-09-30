WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        country,
        TRIM(browser) AS clean_browser
    FROM sessions
    WHERE browser IS NOT NULL
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.web_event_duration_sec,
        x.event_time
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec, c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.clean_browser,
        s.web_event_duration_sec,
        top1.web_event_event_time,
        top1.page
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        clean_browser,
        web_event_duration_sec,
        web_event_event_time,
        page,
        MIN(web_event_duration_sec) OVER w_hist AS running_min_web_event_duration_sec,
        SUM(web_event_duration_sec) OVER w_hist AS running_sum_web_event_duration_sec,
        AVG(web_event_duration_sec) OVER w_hist AS running_avg_web_event_duration_sec,
        COUNT(web_event_duration_sec) OVER w_hist AS running_count_web_event_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY web_event_duration_sec ASC, id ASC) AS row_seq
    FROM with_last
    WINDOW w_hist AS (PARTITION BY page ORDER BY web_event_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Window metrics over several different partitions
SELECT
    id,
    web_event_event_time,
    LAG(running_count_web_event_duration_sec) OVER w_ordered AS prev_running_count_web_event_duration_sec,
    LEAD(row_seq) OVER w_ordered AS next_row_seq,
    LAG(row_seq) OVER w_ordered AS prev_row_seq,
    ROW_NUMBER() OVER w_ordered AS row_seq_2,
    AVG(running_count_web_event_duration_sec) OVER (PARTITION BY page) AS page_avg_running_count_web_event_duration_sec
FROM scored
WINDOW w_ordered AS (PARTITION BY page ORDER BY web_event_event_time ASC, id ASC)
