WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_time,
        SUBSTRING(event_type, 1, 2) AS event_type_prefix,
        COALESCE(page, 'OTHER') AS page_filled
    FROM web_events
    WHERE event_time > DATE '2023-05-04'
      AND page IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS row_seq,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM valid
    WHERE HOUR(event_time) BETWEEN 7 AND 13
    WINDOW w_running AS (ORDER BY event_time, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        page_filled,
        row_seq,
        event_seq,
        AVG(event_seq) OVER (ORDER BY event_seq, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS event_seq_avg_6,
        ROW_NUMBER() OVER (ORDER BY event_seq DESC, id) AS event_seq_2,
        LEAD(event_seq, 2) OVER (ORDER BY row_seq, id) AS next_event_seq,
        NTILE(3) OVER (ORDER BY row_seq, id) AS row_seq_bucket,
        AVG(event_seq) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS event_seq_avg_7d
    FROM scored
),
scored_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(row_seq) OVER (ORDER BY event_seq, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS row_seq_avg_2_prior,
        CUME_DIST() OVER (ORDER BY row_seq_bucket NULLS LAST) AS row_seq_bucket_cume_dist,
        NTILE(4) OVER (ORDER BY event_seq_2, id) AS event_seq_2_bucket
    FROM with_windows
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(row_seq / row_seq_avg_2_prior, 2) AS row_seq_per_row_seq_avg_2_prior
    FROM scored_web_events
    WHERE next_event_seq = 5
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq_2_bucket >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_2_bucket_tier,
    CASE WHEN row_seq_avg_2_prior >= 9 THEN 'HIGH' ELSE 'LOW' END AS row_seq_avg_2_prior_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(event_seq_2) AS total_event_seq_2,
    GROUPING_ID() AS grouping_level
FROM cleaned
GROUP BY GROUPING SETS ((CASE WHEN event_seq_2_bucket >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN row_seq_avg_2_prior >= 9 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN event_seq_2_bucket >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY row_seq_avg_2_prior_tier DESC
