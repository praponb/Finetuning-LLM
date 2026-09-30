WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        DATE_FORMAT(starts_on, 'yyyy-MM-dd') AS starts_on_label,
        SUBSTRING(promo_code, 1, 2) AS promo_code_prefix
    FROM promotions
    WHERE starts_on > DATE '2023-05-12'
      AND channel IS NOT NULL
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '14' DAY
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM attributed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY starts_on ASC, id ASC, promo_id ASC) = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        promo_id,
        channel,
        discount_pct,
        promotion_discount_pct
    FROM deduped
) src
UNPIVOT (
    val FOR metric IN (discount_pct AS DISCOUNT_PCT_VAL, promotion_discount_pct AS PROMOTION_DISCOUNT_PCT)
);
