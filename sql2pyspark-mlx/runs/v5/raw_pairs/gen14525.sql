WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(channel, TRUE) OVER (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        LAG(discount_pct, 1, 0) OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS prev_discount_pct
    FROM promotions
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(prev_discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_prev_discount_pct,
        AVG(prev_discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_prev_discount_pct,
        MAX(prev_discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_prev_discount_pct
    FROM metrics
    WHERE YEAR(starts_on) >= 2025
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(discount_pct AS INT) AS discount_pct_int,
        CASE WHEN channel IN ('web', 'partner') THEN 'Tier 1' ELSE 'Other' END AS channel_group
    FROM with_windows
    WHERE promo_code = 'SPRING10'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        running_sum_prev_discount_pct,
        running_max_prev_discount_pct,
        discount_pct_int,
        channel_group
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC, id) AS rn
        FROM cleaned
    ) x
    WHERE rn = 1
),
with_windows_promotions AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(running_sum_prev_discount_pct / SUM(running_sum_prev_discount_pct) OVER (PARTITION BY channel), 4) AS running_sum_prev_discount_pct_share,
        AVG(running_sum_prev_discount_pct) OVER (PARTITION BY channel) AS channel_avg_running_sum_prev_discount_pct,
        RANK() OVER (PARTITION BY channel ORDER BY running_max_prev_discount_pct DESC) AS running_max_prev_discount_pct_rank
    FROM deduped
)
SELECT
    id,
    channel,
    starts_on,
    ends_on,
    running_sum_prev_discount_pct,
    discount_pct_int,
    channel_group,
    channel_avg_running_sum_prev_discount_pct,
    CASE WHEN channel RLIKE '^(web|store)$' THEN 'PRIORITY' WHEN channel RLIKE '^(mobile|partner)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS channel_zone
FROM with_windows_promotions
ORDER BY starts_on DESC NULLS LAST;
