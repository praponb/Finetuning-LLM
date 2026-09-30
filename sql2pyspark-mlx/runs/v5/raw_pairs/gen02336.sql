WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(event_type, ' - ', page) AS event_type_page
    FROM web_events
    WHERE event_time > DATE '2023-07-02'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        event_type_page,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_avg_6
    FROM cleaned
),
matched_keys AS (
    -- Set difference on keys
    SELECT session_id
    FROM scored
    EXCEPT
    SELECT session_id
    FROM web_events
    WHERE event_type NOT IN ('purchase', 'add_to_cart', 'search')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN matched_keys k ON s.session_id = k.session_id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN duration_sec_avg_6 >= 406 THEN 'High' WHEN duration_sec_avg_6 >= 101 THEN 'Medium' ELSE 'Low' END AS duration_sec_avg_6_level
    FROM reconciled
    WHERE running_avg_duration_sec <= 250
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        d.browser,
        d.country,
        d.device
    FROM filtered s
    LEFT JOIN sessions d
        ON s.session_id = d.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    device,
    metric,
    value
FROM (
    SELECT
        device,
        running_max_duration_sec,
        running_min_duration_sec,
        running_avg_duration_sec
    FROM with_dims
) src
UNPIVOT (
    value FOR metric IN (running_max_duration_sec, running_min_duration_sec AS RUNNING_MIN_DURATION_SEC, running_avg_duration_sec AS RUNNING_AVG_DURATION_SEC)
) unp
