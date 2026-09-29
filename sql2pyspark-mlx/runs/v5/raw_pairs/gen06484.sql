WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(discount_pct AS INT) discount_pct_int,
        COALESCE(channel, 'DIRECT') channel_clean
    FROM promotions
    WHERE starts_on > TO_DATE('2023-07-03')
      AND channel IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        ends_on,
        channel_clean,
        FIRST_VALUE(channel, TRUE) OVER (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_channel,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_channel
    FROM valid
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        ends_on,
        ROW_NUMBER() OVER w_hist event_seq,
        ROW_NUMBER() OVER w_running row_seq
    FROM metrics
    WINDOW w_hist AS (ORDER BY ends_on, id), w_running AS (ORDER BY ends_on DESC, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ends_on,
        row_seq,
        LEAD(row_seq, 2) OVER (ORDER BY ends_on, id) next_row_seq,
        LAST_VALUE(event_seq, TRUE) OVER (ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_event_seq
    FROM enriched
),
metrics_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(last_known_event_seq) OVER w_part running_sum_last_known_event_seq,
        AVG(last_known_event_seq) OVER w_part running_avg_last_known_event_seq,
        MIN(last_known_event_seq) OVER w_part running_min_last_known_event_seq,
        MAX(last_known_event_seq) OVER w_part running_max_last_known_event_seq
    FROM with_windows
    WHERE next_row_seq <> 6
    WINDOW w_part AS (ORDER BY last_known_event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        ends_on,
        row_seq,
        next_row_seq,
        last_known_event_seq,
        running_avg_last_known_event_seq,
        running_min_last_known_event_seq,
        running_max_last_known_event_seq,
        CAST(last_known_event_seq AS DOUBLE) last_known_event_seq_str,
        COALESCE(last_known_event_seq, 0) last_known_event_seq_filled
    FROM metrics_promotions
    WHERE ends_on >= DATE '2023-07-19'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        ends_on,
        row_seq,
        next_row_seq,
        last_known_event_seq,
        running_avg_last_known_event_seq,
        running_max_last_known_event_seq,
        last_known_event_seq_str,
        last_known_event_seq_filled,
        ROUND(last_known_event_seq_str / next_row_seq, 2) last_known_event_seq_str_per_next_row_seq,
        last_known_event_seq + last_known_event_seq_str last_known_event_seq_plus_last_known_event_seq_str
    FROM valid_promotions
    WHERE ends_on >= '2025-07-01'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', ends_on) AS year_start,
    CASE WHEN next_row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS next_row_seq_tier,
    DATE_TRUNC('WEEK', ends_on) AS week_start,
    AVG(row_seq) AS avg_row_seq,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS event_count
FROM base
GROUP BY DATE_TRUNC('YEAR', ends_on), CASE WHEN next_row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', ends_on)
HAVING event_count < 1000;
