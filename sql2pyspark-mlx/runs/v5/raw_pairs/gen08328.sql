WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device,
        browser,
        country,
        CONCAT(country, ' ', browser) country_browser
    FROM sessions
    WHERE started_at > DATE '2023-03-27'
      AND device NOT IN ('tablet', 'mobile')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        country,
        country_browser,
        SUBSTRING(device, 1, 2) device_prefix,
        COALESCE(country, 'N/A') country_filled
    FROM cleaned
    WHERE device = 'mobile'
      AND country IS NOT NULL
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.event_type,
        top1.web_event_duration_sec
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.event_type, c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        country_browser,
        country_filled,
        MAX(web_event_duration_sec) OVER w_hist running_max_web_event_duration_sec,
        MIN(web_event_duration_sec) OVER w_hist running_min_web_event_duration_sec,
        AVG(web_event_duration_sec) OVER w_hist running_avg_web_event_duration_sec,
        SUM(web_event_duration_sec) OVER w_hist running_sum_web_event_duration_sec
    FROM latest_match
    WINDOW w_hist AS (PARTITION BY device ORDER BY web_event_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT device
    FROM scored
    INTERSECT ALL
    SELECT device
    FROM sessions
    WHERE started_at < '2024-06-02'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN remaining_keys k ON s.device = k.device
),
report AS (
    SELECT
        device,
        country_browser,
        country_filled,
        running_max_web_event_duration_sec,
        running_min_web_event_duration_sec,
        running_avg_web_event_duration_sec,
        running_sum_web_event_duration_sec
    FROM verified
    ORDER BY running_sum_web_event_duration_sec ASC NULLS LAST
)
SELECT
    device,
    country_browser,
    running_min_web_event_duration_sec,
    running_avg_web_event_duration_sec,
    running_sum_web_event_duration_sec
FROM report
