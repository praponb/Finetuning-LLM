WITH base AS (
    SELECT *, CASE WHEN promo_code = 'BUNDLE' THEN 1 ELSE 0 END is_bundle
    FROM promotions
    WHERE starts_on > DATE '2023-07-09'
      AND channel IS NOT NULL
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY is_bundle, id) AS row_num
        FROM base
    ) x
    WHERE row_num = 1
),
attributed AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM deduped src
    INNER JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on <= DATE_ADD(pr.ends_on, 3)
),
cleaned AS (
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        is_bundle,
        row_num,
        promo_id,
        promotion_promo_code,
        TRIM(promotion_promo_code) clean_promotion_promo_code,
        COALESCE(channel, 'OTHER') resolved_channel
    FROM attributed
    WHERE promotion_promo_code IS NOT NULL
)
SELECT
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct,
    is_bundle,
    row_num,
    promo_id,
    promotion_promo_code,
    clean_promotion_promo_code,
    resolved_channel,
    AVG(promo_id) OVER w_hist promo_id_avg_6,
    LEAD(discount_pct, 2) OVER w_recent next_discount_pct
FROM cleaned
WINDOW w_hist AS (PARTITION BY promo_code ORDER BY row_num, id, promo_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY channel ORDER BY discount_pct, id, promo_id);
