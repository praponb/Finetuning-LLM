WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DAY(ends_on) AS ends_on_day,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM promotions
    WHERE starts_on > DATE '2023-10-06'
      AND channel IN ('mobile', 'web')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY channel ORDER BY ends_on) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount_pct,
        MAX(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount_pct
    FROM latest
    WHERE MONTH(ends_on) IN (1, 5, 12)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        resolved_channel,
        running_avg_discount_pct,
        LEAD(running_max_discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS next_running_max_discount_pct,
        LEAD(running_avg_discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY running_sum_discount_pct, id) AS next_running_avg_discount_pct,
        SUM(ends_on_day) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_ends_on_day
    FROM metrics
)
-- Grouped metrics
SELECT
    promo_code,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END) AS welcome5_count,
    ROUND(SUM(next_running_max_discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END), 0), 2) AS next_running_max_discount_pct_per_spring10
FROM with_windows
GROUP BY promo_code
HAVING ROUND(SUM(next_running_max_discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END), 0), 2) < 1000000
ORDER BY promo_code
