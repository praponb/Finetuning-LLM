WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN discount_pct >= 0.3 THEN 'High' WHEN discount_pct >= 0.14 THEN 'Medium' ELSE 'Low' END AS discount_pct_tier,
        DAY(starts_on) AS starts_on_day,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
      AND MONTH(starts_on) IN (3, 9, 12)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_min_discount_pct,
        SUM(starts_on_day) OVER (ORDER BY discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_starts_on_day
    FROM base
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, channel
    FROM metrics
    EXCEPT DISTINCT
    SELECT id, channel
    FROM promotions
    WHERE promo_code = 'WELCOME5'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.channel = k.channel
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, PERCENT_RANK() OVER w_running AS cumulative_starts_on_day_percent_rank
    FROM reconciled
    WINDOW w_running AS (PARTITION BY promo_code ORDER BY cumulative_starts_on_day ASC)
)
-- Grouped metrics
SELECT
    CASE WHEN cumulative_starts_on_day >= 418 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_starts_on_day_tier,
    SUM(cumulative_starts_on_day_percent_rank) FILTER (WHERE promo_code = 'WELCOME5') AS welcome5_total_cumulative_starts_on_day_percent_rank,
    ROUND(SUM(starts_on_day) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS starts_on_day_per_mobile,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(cumulative_starts_on_day) / NULLIF(COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END), 0), 2) AS cumulative_starts_on_day_per_welcome5
FROM with_windows
GROUP BY CASE WHEN cumulative_starts_on_day >= 418 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY welcome5_total_cumulative_starts_on_day_percent_rank ASC;
