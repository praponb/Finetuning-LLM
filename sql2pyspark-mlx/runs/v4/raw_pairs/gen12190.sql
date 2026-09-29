WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS next_discount_pct,
        NTILE(10) OVER w_hist AS discount_pct_bucket,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_pct_avg_6,
        LEAD(discount_pct, 2) OVER (PARTITION BY channel ORDER BY starts_on, id) AS next_discount_pct_2
    FROM promotions
    WINDOW w_hist AS (PARTITION BY channel ORDER BY discount_pct, id)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM enriched e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on <= DATE_ADD(pr.ends_on, 10)
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    id,
    promo_code,
    starts_on,
    discount_pct,
    next_discount_pct,
    discount_pct_bucket,
    discount_pct_avg_6,
    promo_id,
    promo_id_2
FROM matched_promotions
ORDER BY promo_id NULLS LAST, starts_on DESC;
