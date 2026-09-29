WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on, id, promo_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS discount_pct_avg_2,
        PERCENT_RANK() OVER (PARTITION BY promotion_promo_code ORDER BY promo_id NULLS LAST) AS promo_id_percent_rank,
        ROW_NUMBER() OVER (ORDER BY discount_pct DESC, id, promo_id) AS row_seq
    FROM with_window
),
enriched_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY channel), 4) AS promo_id_share,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY discount_pct_avg_2 NULLS LAST) AS discount_pct_avg_2_percent_rank
    FROM enriched
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched_promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY promo_id_share, id, promo_id) = 1
),
latest_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_avg_2 DESC NULLS LAST, id, promo_id) AS row_num
        FROM latest
    ) t
    WHERE row_num = 1
)
SELECT
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct,
    promo_id,
    promotion_discount_pct,
    discount_pct_avg_2,
    promo_id_percent_rank,
    CASE WHEN promo_code RLIKE '^(BUNDLE|SPRING10)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS promo_code_zone
FROM latest_promotions
ORDER BY channel, discount_pct_avg_2
