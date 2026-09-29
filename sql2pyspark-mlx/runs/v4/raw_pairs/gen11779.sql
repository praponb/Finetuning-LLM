WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id) = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM deduped src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on < pr.ends_on + INTERVAL '14' DAY
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM attributed
    EXCEPT DISTINCT
    SELECT id
    FROM promotions
    WHERE MONTH(starts_on) IN (7, 8, 11)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM attributed s
    INNER JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        promo_code,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        MONTH(ends_on) ends_on_month
    FROM kept_rows
    WHERE promo_code = 'VIP'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY promotion_promo_code), 4) promo_id_share,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) discount_pct_share,
        COUNT(discount_pct) OVER (PARTITION BY promo_code) promo_code_count_discount_pct
    FROM base
)
SELECT
    promo_code,
    ends_on,
    promo_id,
    promo_id_share,
    discount_pct_share,
    CASE WHEN promo_code RLIKE '^(WELCOME5|SPRING10)$' THEN 'CORE' WHEN promo_code RLIKE '^(VIP|BUNDLE)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS promo_code_zone
FROM scored
ORDER BY ends_on DESC NULLS FIRST;
