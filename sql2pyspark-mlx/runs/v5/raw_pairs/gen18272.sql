WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        ends_on,
        discount_pct,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM promotions
    WHERE starts_on >= DATE '2023-07-13'
      AND channel IS DISTINCT FROM 'web'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY days_since_ends_on, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(days_since_ends_on) OVER w_ordered AS running_max_days_since_ends_on,
        COUNT(days_since_ends_on) OVER w_ordered AS running_count_days_since_ends_on,
        AVG(days_since_ends_on) OVER w_ordered AS running_avg_days_since_ends_on,
        MIN(days_since_ends_on) OVER w_ordered AS running_min_days_since_ends_on
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY channel ORDER BY days_since_ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        days_since_ends_on,
        channel_filled,
        running_max_days_since_ends_on,
        running_count_days_since_ends_on,
        running_min_days_since_ends_on,
        LAST_VALUE(days_since_ends_on, TRUE) OVER w_part AS last_known_days_since_ends_on,
        LAST_VALUE(running_avg_days_since_ends_on, TRUE) OVER w_part_2 AS last_known_running_avg_days_since_ends_on
    FROM enriched
    WINDOW w_part AS (ORDER BY running_min_days_since_ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY channel ORDER BY running_count_days_since_ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base_promotions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(days_since_ends_on * 1.07, 2) AS days_since_ends_on_adjusted,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM scored
    WHERE channel IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'partner'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(running_max_days_since_ends_on) OVER (PARTITION BY channel ORDER BY days_since_ends_on_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_max_days_since_ends_on,
        SUM(last_known_running_avg_days_since_ends_on) OVER (PARTITION BY channel ORDER BY last_known_days_since_ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_running_avg_days_since_ends_on,
        FIRST_VALUE(last_known_days_since_ends_on) IGNORE NULLS OVER (ORDER BY last_known_days_since_ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_known_days_since_ends_on
    FROM base_promotions
    WHERE days_since_ends_on_adjusted > 65
)
-- Running and moving aggregates
SELECT
    *,
    SUM(last_known_running_avg_days_since_ends_on) OVER (PARTITION BY channel ORDER BY running_min_days_since_ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_running_avg_days_since_ends_on_2,
    NTILE(10) OVER w_running AS days_since_ends_on_adjusted_bucket,
    LEAD(running_running_max_days_since_ends_on, 1) OVER w_ordered AS next_running_running_max_days_since_ends_on,
    RANK() OVER (PARTITION BY channel ORDER BY first_known_last_known_days_since_ends_on DESC NULLS FIRST) AS first_known_last_known_days_since_ends_on_rank
FROM with_windows
WINDOW w_running AS (PARTITION BY channel ORDER BY days_since_ends_on_adjusted, id), w_ordered AS (PARTITION BY channel ORDER BY running_max_days_since_ends_on, id);
