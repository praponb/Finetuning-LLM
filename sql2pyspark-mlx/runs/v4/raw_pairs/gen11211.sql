WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(channel, TRUE) OVER w_ordered AS first_known_channel,
        PERCENT_RANK() OVER (ORDER BY discount_pct) AS discount_pct_percent_rank
    FROM promotions
    WINDOW w_ordered AS (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct_percent_rank DESC NULLS FIRST, id) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, promo_code
    FROM ranked
    EXCEPT DISTINCT
    SELECT id, promo_code
    FROM promotions
    WHERE promo_code NOT IN ('WELCOME5', 'BUNDLE', 'VIP')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.id = k.id AND s.promo_code = k.promo_code
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM reconciled s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL 14 DAYS
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN promo_id >= 11 THEN 'HIGH' ELSE 'LOW' END AS promo_id_tier,
    CASE WHEN discount_pct >= 0.28 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    MAX(discount_pct) AS max_discount_pct,
    COUNT(CASE WHEN promotion_promo_code = 'BUNDLE' THEN 1 END) AS bundle_count,
    PERCENTILE_APPROX(promotion_discount_pct, 0.5) AS promotion_discount_pct_p50
FROM matched
GROUP BY CASE WHEN promo_id >= 11 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN discount_pct >= 0.28 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY promo_id_tier;
