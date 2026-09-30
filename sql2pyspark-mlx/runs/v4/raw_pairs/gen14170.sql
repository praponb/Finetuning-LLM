WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        promo_code,
        ends_on,
        LOWER(channel) AS channel_lower,
        DATE_TRUNC('WEEK', ends_on) AS ends_on_period
    FROM promotions
    WHERE promo_code != 'VIP'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN ends_on_period - LAG(ends_on_period) OVER (PARTITION BY promo_code ORDER BY ends_on_period ASC, id ASC) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY promo_code ORDER BY ends_on_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(ends_on_period) OVER (PARTITION BY promo_code ORDER BY ends_on_period ASC, id ASC) IS NULL
                 OR ends_on_period > LAG(ends_on_period) OVER (PARTITION BY promo_code ORDER BY ends_on_period ASC, id ASC) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY promo_code ORDER BY ends_on_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(new_group_flag, TRUE) OVER (PARTITION BY promo_code ORDER BY ends_on_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_new_group_flag,
        PERCENT_RANK() OVER w_recent AS island_id_percent_rank,
        AVG(session_start_flag) OVER w_recent_2 AS session_start_flag_avg_2_prior,
        LAST_VALUE(new_group_flag, TRUE) OVER w_hist AS last_known_new_group_flag
    FROM islands
    WINDOW w_recent AS (ORDER BY island_id DESC), w_recent_2 AS (PARTITION BY promo_code ORDER BY ends_on_period ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_hist AS (PARTITION BY promo_code ORDER BY island_id ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        ends_on,
        channel_lower,
        ends_on_period,
        island_id,
        session_start_flag,
        burst_id,
        first_known_new_group_flag,
        island_id_percent_rank,
        LAST_DAY(ends_on) AS ends_on_month_end
    FROM with_windows
    WHERE ends_on > DATE '2023-06-23'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(first_known_new_group_flag, 1) OVER (ORDER BY burst_id ASC, id ASC) AS next_first_known_new_group_flag,
        MAX(first_known_new_group_flag) OVER (ORDER BY CAST(ends_on_period AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS first_known_new_group_flag_max_7d
    FROM filtered
),
with_windows_promotions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(next_first_known_new_group_flag) OVER w_running AS next_first_known_new_group_flag_avg_2_prior,
        LEAD(island_id, 2, 0) OVER (ORDER BY island_id_percent_rank ASC, id ASC) AS next_island_id,
        SUM(island_id_percent_rank) OVER (ORDER BY session_start_flag ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_island_id_percent_rank,
        LEAD(island_id, 1) OVER (ORDER BY ends_on_period ASC, id ASC) AS next_island_id_2,
        COUNT(burst_id) OVER (ORDER BY CAST(ends_on_period AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS burst_id_count_90d
    FROM scored
    WINDOW w_running AS (ORDER BY ends_on_period ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
grouped AS (
    -- Grouped metrics
    SELECT
        island_id,
        burst_id,
        APPROX_PERCENTILE(next_first_known_new_group_flag_avg_2_prior, ARRAY(0.25, 0.5, 0.75)) AS next_first_known_new_group_flag_avg_2_prior_quartiles,
        (UNIX_TIMESTAMP(MAX(ends_on_period)) - UNIX_TIMESTAMP(MIN(ends_on_period))) / 60 AS duration_min,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows_promotions
    GROUP BY island_id, burst_id
)
SELECT
    island_id,
    burst_id,
    next_first_known_new_group_flag_avg_2_prior_quartiles,
    duration_min,
    distinct_ids
FROM grouped
ORDER BY burst_id ASC NULLS FIRST, distinct_ids DESC
