WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN discount_pct >= 0.34 THEN 'High' WHEN discount_pct >= 0.12 THEN 'Medium' ELSE 'Low' END AS discount_pct_tier,
        DATE_FORMAT(starts_on, 'yyyy-MM-dd') AS starts_on_label
    FROM promotions
    WHERE ends_on >= DATE '2023-06-29'
      AND channel = 'web'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_tier,
        starts_on_label,
        DATE_FORMAT(ends_on, 'yyyy-MM-dd') AS ends_on_label,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM cleaned
    WHERE starts_on >= DATE '2023-06-22'
      AND channel IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior,
        SUM(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_2
    FROM filtered
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM metrics s
    INNER JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 3)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        promo_id,
        promo_code,
        promotion_discount_pct,
        discount_pct
    FROM matched
) src
UNPIVOT (
    value FOR metric_name IN (promotion_discount_pct AS PROMOTION_DISCOUNT_PCT, discount_pct AS DISCOUNT_PCT_VAL)
);
