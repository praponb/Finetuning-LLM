WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(promo_code, ' - ', channel) AS promo_code_channel
    FROM promotions
    WHERE channel IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(discount_pct) OVER w_recent AS last4_min_discount_pct,
        SUM(discount_pct) OVER w_recent AS last4_sum_discount_pct,
        AVG(discount_pct) OVER w_recent AS last4_avg_discount_pct,
        MAX(discount_pct) OVER w_recent AS last4_max_discount_pct,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY discount_pct) AS discount_pct_cume_dist
    FROM valid
    WHERE channel = 'web'
    WINDOW w_recent AS (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_windows_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(last4_max_discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_last4_max_discount_pct,
        AVG(last4_max_discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_last4_max_discount_pct,
        MAX(last4_max_discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_last4_max_discount_pct,
        COUNT(last4_max_discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_last4_max_discount_pct
    FROM with_windows
),
with_windows_promotions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(discount_pct) OVER w_running AS next_discount_pct,
        LAG(discount_pct) OVER w_running AS prev_discount_pct,
        LAG(last7_count_last4_max_discount_pct) OVER w_running AS prev_last7_count_last4_max_discount_pct,
        LEAD(last7_count_last4_max_discount_pct) OVER w_running AS next_last7_count_last4_max_discount_pct,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM with_windows_promotions
    WINDOW w_running AS (PARTITION BY promo_code ORDER BY last7_count_last4_max_discount_pct, id)
)
SELECT
    id,
    channel,
    ends_on,
    discount_pct,
    promo_code_channel,
    last4_min_discount_pct,
    last4_avg_discount_pct,
    discount_pct_cume_dist,
    last7_avg_last4_max_discount_pct,
    CASE WHEN channel RLIKE '^(web|store)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS channel_zone
FROM with_windows_promotions_2
ORDER BY last7_avg_last4_max_discount_pct DESC;
