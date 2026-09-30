WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(discount_pct) AS discount_pct_whole,
        UPPER(promo_code) AS promo_code_upper
    FROM promotions
    WHERE channel IS NOT DISTINCT FROM 'mobile'
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on < pr.ends_on + INTERVAL '30' DAY
),
attributed_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL 30 DAYS
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id, promo_id, promo_id_2) AS latest_rank
    FROM attributed_promotions
    QUALIFY latest_rank = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_SUB(starts_on, 52) AS starts_on_minus_52d
    FROM latest
    WHERE promotion_promo_code IS NOT NULL
)
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
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'PRIORITY' WHEN channel RLIKE '^(web|mobile)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS channel_zone
FROM filtered
ORDER BY starts_on
