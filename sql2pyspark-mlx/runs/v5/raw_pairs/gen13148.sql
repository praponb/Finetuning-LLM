WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code, channel ORDER BY starts_on DESC) AS rn
        FROM promotions
    ) t
    WHERE rn = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM latest s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL 7 DAYS
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct, id, promo_id, promo_id_2 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_discount_pct,
        MAX(discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct, id, promo_id, promo_id_2 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct, id, promo_id, promo_id_2 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_discount_pct
    FROM matched_promotions
),
latest_promotions AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promo_id_2,
        promotion_promo_code_2,
        promotion_discount_pct_2,
        last3_max_discount_pct,
        last3_sum_discount_pct
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promotion_promo_code, promo_code ORDER BY last3_max_discount_pct DESC) AS latest_rank
        FROM metrics
    ) t
    WHERE latest_rank = 1
)
SELECT
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct,
    promo_id_2,
    CASE WHEN promo_code RLIKE '^(BUNDLE|WELCOME5)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS promo_code_zone
FROM latest_promotions
ORDER BY promotion_promo_code NULLS LAST;
