WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(discount_pct) AS discount_pct_whole
    FROM promotions
    WHERE channel IS DISTINCT FROM 'mobile'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on < pr.ends_on + INTERVAL '14' DAY
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, promo_code
    FROM with_window
    EXCEPT DISTINCT
    SELECT id, promo_code
    FROM promotions
    WHERE ends_on < '2024-12-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_window s
    JOIN matched_keys k ON s.id = k.id AND s.promo_code = k.promo_code
)
SELECT
    promo_code,
    starts_on,
    ends_on,
    discount_pct_whole,
    promotion_promo_code,
    promo_id_2,
    promotion_promo_code_2,
    promotion_discount_pct_2,
    CASE WHEN promo_code RLIKE '^(WELCOME5|BUNDLE)$' THEN 'PRIORITY' WHEN promo_code RLIKE '^(SPRING10|VIP)$' THEN 'GROUP_B' ELSE 'CORE' END AS promo_code_zone
FROM verified
ORDER BY promo_id_2;
