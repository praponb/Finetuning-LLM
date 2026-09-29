WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL 30 DAYS
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        starts_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        LENGTH(promo_code) promo_code_length,
        CASE WHEN discount_pct >= 0.33 THEN 'High' WHEN discount_pct >= 0.12 THEN 'Medium' ELSE 'Low' END discount_pct_band
    FROM matched
    WHERE channel = 'partner'
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct_2
    FROM valid e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL '30' DAY
),
matched_promotions_2 AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_3
    FROM matched_promotions src
    JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(promo_id / promotion_discount_pct_3, 2) promo_id_per_promotion_discount_pct_3,
        COALESCE(promotion_promo_code_2, 'NONE') promotion_promo_code_2_clean
    FROM matched_promotions_2
    WHERE starts_on >= DATE '2023-03-22'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        channel,
        discount_pct,
        promo_id,
        promotion_discount_pct,
        promo_code_length,
        promo_id_2,
        promotion_discount_pct_2,
        promo_id_3,
        promotion_discount_pct_3,
        promo_id_per_promotion_discount_pct_3
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promotion_promo_code_2, promo_code ORDER BY promo_code_length DESC, id, promo_id, promo_id_2, promo_id_3) = 1
)
-- Multi-level aggregation
SELECT
    promo_id,
    CASE WHEN promo_id_3 >= 9 THEN 'LARGE' ELSE 'SMALL' END AS promo_id_3_tier,
    CASE WHEN promo_id_per_promotion_discount_pct_3 >= 62 THEN 'LARGE' ELSE 'SMALL' END AS promo_id_per_promotion_discount_pct_3_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(promo_code_length) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS promo_code_length_per_web,
    AVG(promo_code_length) AS avg_promo_code_length
FROM latest
GROUP BY GROUPING SETS ((promo_id, CASE WHEN promo_id_3 >= 9 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN promo_id_per_promotion_discount_pct_3 >= 62 THEN 'LARGE' ELSE 'SMALL' END), (promo_id, CASE WHEN promo_id_3 >= 9 THEN 'LARGE' ELSE 'SMALL' END))
HAVING promo_code_length_per_web > 0
