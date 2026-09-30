WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(starts_on, ends_on) AS days_ends_on_to_starts_on,
        CASE WHEN channel = 'partner' THEN 1 ELSE 0 END AS is_partner
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-10-02')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        days_ends_on_to_starts_on,
        is_partner,
        LAST_DAY(starts_on) AS starts_on_month_end,
        CAST(discount_pct AS INT) AS discount_pct_int,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM valid
    WHERE days_ends_on_to_starts_on BETWEEN 48 AND 54
      AND channel IS NOT NULL
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered a
    INNER JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.starts_on_month_end >= pr.starts_on
       AND a.starts_on_month_end < pr.ends_on + INTERVAL '30' DAY
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(discount_pct) OVER w_part AS last3_count_discount_pct,
        AVG(discount_pct) OVER w_part AS last3_avg_discount_pct,
        MIN(discount_pct) OVER w_part AS last3_min_discount_pct,
        SUM(discount_pct) OVER w_part AS last3_sum_discount_pct
    FROM matched
    WINDOW w_part AS (PARTITION BY promotion_promo_code ORDER BY is_partner, id, promo_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT channel
    FROM metrics
    EXCEPT
    SELECT channel
    FROM promotions
    WHERE discount_pct BETWEEN 0.18 AND 0.31
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.channel = k.channel
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount_pct >= 0.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    promotion_promo_code,
    APPROX_PERCENTILE(promo_id, 0.9) AS promo_id_p90,
    COUNT(DISTINCT channel) AS distinct_channels,
    AVG(last3_sum_discount_pct) FILTER (WHERE channel = 'web') AS web_avg_last3_sum_discount_pct,
    SIZE(COLLECT_SET(promotion_promo_code)) AS num_promotion_promo_codes
FROM verified
GROUP BY discount_pct_tier, promotion_promo_code;
