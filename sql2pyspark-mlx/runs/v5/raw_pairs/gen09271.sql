WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label
    FROM web_events
    WHERE page IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN page = '/checkout' THEN 1 ELSE 0 END AS is_checkout,
        COALESCE(page, 'NONE') AS page_clean
    FROM filtered
    WHERE event_time > TO_TIMESTAMP('2023-09-05 05:00:00')
      AND page IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        event_time_label,
        is_checkout,
        page_clean,
        CASE WHEN is_checkout >= 71 THEN 'High' WHEN is_checkout >= 33 THEN 'Medium' ELSE 'Low' END AS is_checkout_band
    FROM base
    WHERE event_type = 'search'
      AND event_time >= TIMESTAMP '2023-07-21 08:00:00'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(is_checkout) OVER (PARTITION BY event_type ORDER BY is_checkout, id) AS prev_is_checkout,
        LAG(duration_sec) OVER (PARTITION BY event_type ORDER BY is_checkout, id) AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY is_checkout, id) AS seq_no,
        STDDEV_SAMP(is_checkout) OVER (PARTITION BY event_type) AS event_type_stddev_samp_is_checkout
    FROM valid
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(seq_no, 1) OVER (PARTITION BY event_type, page ORDER BY duration_sec, id) AS prev_seq_no,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY seq_no NULLS FIRST) AS seq_no_dense_rank,
        MAX(prev_duration_sec) OVER (PARTITION BY event_type) AS event_type_max_prev_duration_sec,
        SUM(prev_is_checkout) OVER (PARTITION BY page ORDER BY is_checkout, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_is_checkout
    FROM with_windows
)
SELECT
    id,
    event_type,
    page,
    duration_sec,
    event_time_label,
    is_checkout,
    is_checkout_band,
    prev_is_checkout,
    prev_duration_sec
FROM scored
ORDER BY prev_is_checkout DESC NULLS FIRST
