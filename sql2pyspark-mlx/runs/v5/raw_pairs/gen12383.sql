WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ADD_MONTHS(ends_on, 6) ends_on_plus_months,
        ROUND(discount_pct, 0) discount_pct_rounded
    FROM promotions
    WHERE channel = 'store'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        ends_on_plus_months,
        discount_pct_rounded,
        CASE WHEN channel IN ('web', 'partner') THEN 'Group A' ELSE 'Other' END channel_group
    FROM cleaned
    WHERE MONTH(ends_on_plus_months) IN (1, 3, 10)
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 7)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        channel_group,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code, promo_code ORDER BY promo_id, id, promo_id) row_seq,
        NTILE(4) OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct DESC, id, promo_id) promotion_discount_pct_bucket,
        LAST_VALUE(discount_pct_rounded) IGNORE NULLS OVER w_recent last_known_discount_pct_rounded,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_rounded, id, promo_id) row_seq_2,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY promotion_promo_code, promo_code), 4) promo_id_share
    FROM attributed
    WHERE MONTH(ends_on) IN (1, 2, 5)
    WINDOW w_recent AS (PARTITION BY channel ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_2_tier,
    SUM(CASE WHEN promotion_promo_code = 'SPRING10' THEN promo_id ELSE 0 END) AS spring10_promo_id,
    ROUND(SUM(promo_id) / NULLIF(COUNT(CASE WHEN promotion_promo_code = 'SPRING10' THEN 1 END), 0), 2) AS promo_id_per_spring10,
    COUNT(*) AS num_rows,
    AVG(promotion_discount_pct) FILTER (WHERE promotion_promo_code = 'SPRING10') AS spring10_avg_promotion_discount_pct,
    SUM(CASE WHEN promo_code = 'SPRING10' THEN promo_id ELSE 0 END) AS spring10_promo_id_2,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes
FROM metrics
GROUP BY CASE WHEN row_seq_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING SIZE(COLLECT_SET(promo_code)) > 1
ORDER BY spring10_avg_promotion_discount_pct DESC;
