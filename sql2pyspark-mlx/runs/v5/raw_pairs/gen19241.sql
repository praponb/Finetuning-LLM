WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
kept_keys AS (
    -- Set difference on keys
    SELECT channel, promo_code
    FROM with_window
    EXCEPT
    SELECT channel, promo_code
    FROM promotions
    WHERE MONTH(starts_on) IN (8, 9, 11)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_window s
    INNER JOIN kept_keys k ON s.channel = k.channel AND s.promo_code = k.promo_code
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        promotion_promo_code,
        CUME_DIST() OVER (ORDER BY promotion_discount_pct DESC) AS promotion_discount_pct_cume_dist
    FROM reconciled
),
scored_promotions AS (
    -- Ranking and gap features
    SELECT
        channel,
        promo_code,
        promotion_promo_code,
        promotion_discount_pct_cume_dist,
        STDDEV_SAMP(promotion_discount_pct_cume_dist) OVER (PARTITION BY promotion_promo_code) AS promotion_promo_code_stddev_samp_promotion_discount_pct_cume_dist,
        CUME_DIST() OVER (ORDER BY promotion_discount_pct_cume_dist) AS promotion_discount_pct_cume_dist_cume_dist,
        MIN(promotion_discount_pct_cume_dist) OVER (PARTITION BY channel, promotion_promo_code) AS channel_min_promotion_discount_pct_cume_dist
    FROM scored
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY promotion_discount_pct_cume_dist_cume_dist DESC NULLS FIRST) AS promotion_discount_pct_cume_dist_cume_dist_percent_rank,
        DENSE_RANK() OVER (ORDER BY promotion_discount_pct_cume_dist_cume_dist DESC) AS promotion_discount_pct_cume_dist_cume_dist_dense_rank,
        MAX(promotion_discount_pct_cume_dist_cume_dist) OVER (PARTITION BY promo_code) AS promo_code_max_promotion_discount_pct_cume_dist_cume_dist
    FROM scored_promotions
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        promo_code,
        promotion_discount_pct_cume_dist,
        promotion_promo_code_stddev_samp_promotion_discount_pct_cume_dist,
        channel_min_promotion_discount_pct_cume_dist,
        promotion_discount_pct_cume_dist_cume_dist_percent_rank,
        promotion_discount_pct_cume_dist_cume_dist_dense_rank,
        LENGTH(promotion_promo_code) AS promotion_promo_code_length,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM enriched
    WHERE promotion_promo_code IS NOT NULL
      AND promotion_discount_pct_cume_dist_cume_dist > 0.37
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN promotion_promo_code_length >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS promotion_promo_code_length_tier,
        CASE WHEN channel_min_promotion_discount_pct_cume_dist >= 1 THEN 'HIGH' ELSE 'LOW' END AS channel_min_promotion_discount_pct_cume_dist_tier,
        COUNT(*) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_rows,
        COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END) AS spring10_count,
        SUM(promotion_discount_pct_cume_dist_cume_dist_dense_rank) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_total_promotion_discount_pct_cume_dist_cume_dist_dense_rank,
        SUM(CASE WHEN promo_code = 'WELCOME5' THEN promotion_discount_pct_cume_dist_cume_dist_dense_rank ELSE 0 END) AS welcome5_promotion_discount_pct_cume_dist_cume_dist_dense_rank
    FROM filtered
    GROUP BY CASE WHEN promotion_promo_code_length >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN channel_min_promotion_discount_pct_cume_dist >= 1 THEN 'HIGH' ELSE 'LOW' END
    HAVING SUM(promotion_discount_pct_cume_dist_cume_dist_dense_rank) FILTER (WHERE promo_code = 'BUNDLE') > 0
)
SELECT
    promotion_promo_code_length_tier,
    channel_min_promotion_discount_pct_cume_dist_tier,
    bundle_rows,
    spring10_count,
    bundle_total_promotion_discount_pct_cume_dist_cume_dist_dense_rank,
    welcome5_promotion_discount_pct_cume_dist_cume_dist_dense_rank
FROM summary
ORDER BY spring10_count NULLS LAST;
