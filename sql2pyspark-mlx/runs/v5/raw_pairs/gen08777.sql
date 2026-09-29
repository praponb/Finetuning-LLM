WITH filtered AS (
    SELECT
        *,
        DATEDIFF(starts_on, ends_on) AS days_ends_on_to_starts_on,
        LENGTH(promo_code) AS promo_code_length,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
),
with_window AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 3)
),
attributed AS (
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_window a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on < pr.ends_on + INTERVAL 30 DAYS
),
matched AS (
    SELECT
        a.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM attributed a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
with_windows AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY promotion_promo_code ORDER BY promotion_discount_pct_3, id, promo_id, promo_id_2, promo_id_3) AS promotion_discount_pct_3_bucket,
        ROW_NUMBER() OVER (ORDER BY days_ends_on_to_starts_on DESC, id, promo_id, promo_id_2, promo_id_3) AS row_seq,
        AVG(promotion_discount_pct_3) OVER (PARTITION BY promotion_promo_code, promotion_promo_code_2) AS promotion_promo_code_avg_promotion_discount_pct_3
    FROM matched
),
with_windows_promotions AS (
    SELECT
        *,
        AVG(promo_code_length) OVER (ORDER BY promo_id_3, id, promo_id, promo_id_2, promo_id_3 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS promo_code_length_avg_3_prior,
        LAG(promotion_discount_pct, 1) OVER (PARTITION BY promotion_promo_code_2 ORDER BY promotion_promo_code_avg_promotion_discount_pct_3, id, promo_id, promo_id_2, promo_id_3) AS prev_promotion_discount_pct,
        NTILE(3) OVER (ORDER BY promo_id DESC, id, promo_id, promo_id_2, promo_id_3) AS promo_id_bucket,
        FIRST_VALUE(promo_id_2) IGNORE NULLS OVER (PARTITION BY promotion_promo_code_3, channel ORDER BY discount_pct, id, promo_id, promo_id_2, promo_id_3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_promo_id_2
    FROM with_windows
),
attributed_promotions AS (
    SELECT
        e.*,
        pr.id AS promo_id_4,
        pr.promo_code AS promotion_promo_code_4,
        pr.discount_pct AS promotion_discount_pct_4
    FROM with_windows_promotions e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on <= DATE_ADD(pr.ends_on, 7)
)
SELECT
    id,
    promo_id,
    promo_id_2,
    promo_id_3,
    promo_id_4,
    promotion_promo_code_3,
    metric_name,
    val
FROM (
    SELECT
        id,
        promo_id,
        promo_id_2,
        promo_id_3,
        promo_id_4,
        promotion_promo_code_3,
        promotion_discount_pct_2,
        prev_promotion_discount_pct
    FROM attributed_promotions
) src
UNPIVOT (
    val FOR metric_name IN (promotion_discount_pct_2, prev_promotion_discount_pct)
) unp;
