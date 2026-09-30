WITH ranked AS (
    SELECT *
    FROM promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct DESC, id ASC) = 1
),
matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM ranked src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL '30' DAY
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY promotion_promo_code ORDER BY ends_on ASC) AS rn
    FROM matched
    QUALIFY rn = 1
)
SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        discount_pct
    FROM deduped
) src
PIVOT (
    MAX(discount_pct) AS total,
    COUNT(1) AS item_count
    FOR promo_code IN ('SPRING10', 'BUNDLE')
)
