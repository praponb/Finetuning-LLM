WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN discount_pct >= 0.35 THEN 'High' WHEN discount_pct >= 0.15 THEN 'Medium' ELSE 'Low' END AS discount_pct_band,
        TRIM(channel) AS clean_channel
    FROM promotions
    WHERE channel IS NOT NULL
      AND ends_on >= TO_DATE('2023-08-31')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_band,
        clean_channel,
        YEAR(starts_on) AS starts_on_year,
        CASE WHEN channel IN ('partner', 'store') THEN 'Priority' ELSE 'Other' END AS channel_group
    FROM valid
    WHERE channel IS NOT NULL
      AND discount_pct <= 0.29
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL 7 DAYS
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        discount_pct_band,
        clean_channel,
        starts_on_year,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        ROUND(starts_on_year, 1) AS starts_on_year_rounded,
        ROUND(starts_on_year * 1.15, 2) AS starts_on_year_adjusted,
        COALESCE(promotion_promo_code, 'OTHER') AS resolved_promotion_promo_code
    FROM attributed
    WHERE starts_on >= DATE '2023-07-17'
),
base_promotions AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(starts_on_year_adjusted * 1.15, 2) AS adjusted_starts_on_year_adjusted
    FROM base
    WHERE promotion_promo_code IS NOT DISTINCT FROM 'SPRING10'
      AND promotion_promo_code IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        promo_id,
        promo_code,
        adjusted_starts_on_year_adjusted,
        starts_on_year_rounded,
        starts_on_year_adjusted,
        discount_pct
    FROM base_promotions
) src
UNPIVOT INCLUDE NULLS (
    reading FOR kpi IN (adjusted_starts_on_year_adjusted AS ADJUSTED_STARTS_ON_YEAR_ADJUSTED, starts_on_year_rounded AS STARTS_ON_YEAR_ROUNDED, starts_on_year_adjusted, discount_pct AS DISCOUNT_PCT_VAL)
);
