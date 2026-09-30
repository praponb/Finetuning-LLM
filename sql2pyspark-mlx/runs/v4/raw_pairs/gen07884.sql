WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS rn
    FROM promotions
    QUALIFY rn = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        rn,
        CONCAT(promo_code, ' - ', channel) AS promo_code_channel,
        CASE WHEN promo_code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle
    FROM ranked
    WHERE ends_on > TO_DATE('2023-12-08')
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL '30' DAY
),
matched_keys AS (
    -- Set difference on keys
    SELECT channel, id
    FROM matched
    EXCEPT
    SELECT channel, id
    FROM promotions
    WHERE promo_code = 'BUNDLE'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM matched s
    INNER JOIN matched_keys k ON s.channel = k.channel AND s.id = k.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        promo_code,
        promotion_promo_code,
        discount_pct
    FROM reconciled
) src
PIVOT (
    MAX(discount_pct) AS value_sum,
    COUNT(1) AS item_count
    FOR promotion_promo_code IN ('BUNDLE' AS bundle, 'VIP' AS vip)
);
