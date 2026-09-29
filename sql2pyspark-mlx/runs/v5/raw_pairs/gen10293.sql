WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(discount_pct, 0) discount_pct_rounded,
        COALESCE(channel, 'OTHER') resolved_channel
    FROM promotions
    WHERE MONTH(starts_on) IN (4, 9, 12)
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL '7' DAY
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        discount_pct_rounded,
        resolved_channel,
        promo_id,
        promotion_promo_code
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY starts_on) AS latest_rank
        FROM with_window
    ) x
    WHERE latest_rank = 1
),
latest_promotions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct_rounded DESC NULLS FIRST, id, promo_id) AS dedup_rank
        FROM latest
    ) ranked
    WHERE dedup_rank = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct
    FROM latest_promotions s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'store') = pr.channel
       AND s.starts_on BETWEEN pr.starts_on AND pr.ends_on
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN discount_pct >= 0.31 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    COUNT(DISTINCT promotion_promo_code) AS distinct_promotion_promo_codes,
    AVG(discount_pct) FILTER (WHERE promotion_promo_code = 'VIP') AS vip_avg_discount_pct,
    ROUND(SUM(discount_pct_rounded) / NULLIF(COUNT(CASE WHEN promotion_promo_code = 'VIP' THEN 1 END), 0), 2) AS discount_pct_rounded_per_vip,
    COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows,
    COUNT(DISTINCT channel) AS distinct_channels
FROM matched
GROUP BY CUBE (CASE WHEN discount_pct >= 0.31 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY distinct_promotion_promo_codes DESC NULLS LAST;
