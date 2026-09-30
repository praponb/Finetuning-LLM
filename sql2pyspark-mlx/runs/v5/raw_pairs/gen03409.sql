WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ADD_MONTHS(starts_on, 12) AS starts_on_plus_months,
        LAST_DAY(ends_on) AS ends_on_month_end
    FROM promotions
    WHERE ends_on > DATE '2023-07-14'
      AND channel IS DISTINCT FROM 'partner'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id ASC) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        starts_on_plus_months,
        ends_on_month_end,
        latest_rank,
        SUM(discount_pct) OVER (PARTITION BY promo_code, channel ORDER BY starts_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY discount_pct ASC) AS discount_pct_cume_dist
    FROM deduped
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        channel,
        discount_pct,
        starts_on_plus_months,
        cumulative_discount_pct,
        discount_pct_cume_dist
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY cumulative_discount_pct DESC NULLS LAST, id ASC) AS latest_rank_2
        FROM metrics
    ) ranked
    WHERE latest_rank_2 = 1
),
cleaned_promotions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        discount_pct,
        starts_on_plus_months,
        cumulative_discount_pct,
        UPPER(channel) AS channel_upper,
        ROUND(discount_pct * 0.9, 2) AS adjusted_discount_pct,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM ranked
    WHERE MONTH(starts_on_plus_months) IN (3, 4, 8)
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN cumulative_discount_pct >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_discount_pct_tier,
        AVG(cumulative_discount_pct) AS avg_cumulative_discount_pct,
        COUNT(*) AS num_rows,
        MAX(cumulative_discount_pct) AS max_cumulative_discount_pct
    FROM cleaned_promotions
    GROUP BY CASE WHEN cumulative_discount_pct >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING max_cumulative_discount_pct > 0
)
SELECT
    cumulative_discount_pct_tier,
    avg_cumulative_discount_pct,
    num_rows,
    max_cumulative_discount_pct
FROM totals
ORDER BY cumulative_discount_pct_tier DESC NULLS LAST
LIMIT 5
