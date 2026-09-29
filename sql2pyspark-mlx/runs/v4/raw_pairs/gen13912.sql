WITH matched_keys AS (
    -- Set intersection on keys
    SELECT id, promo_code
    FROM promotions
    INTERSECT ALL
    SELECT id, promo_code
    FROM promotions
    WHERE MONTH(starts_on) IN (1, 5, 11)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN matched_keys k ON s.id = k.id AND s.promo_code = k.promo_code
),
kept_keys AS (
    -- Set difference on keys
    SELECT promo_code, channel
    FROM kept_rows
    EXCEPT
    SELECT promo_code, channel
    FROM promotions
    WHERE promo_code = 'BUNDLE'
),
kept_rows_promotions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys k ON s.promo_code = k.promo_code AND s.channel = k.channel
),
remaining_keys AS (
    -- Set difference on keys
    SELECT promo_code, channel
    FROM kept_rows_promotions
    EXCEPT ALL
    SELECT promo_code, channel
    FROM promotions
    WHERE channel = 'partner'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows_promotions s
    INNER JOIN remaining_keys k ON s.promo_code = k.promo_code AND s.channel = k.channel
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share_2
    FROM reconciled
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    discount_pct_share,
    discount_pct_share_2,
    CASE WHEN promo_code RLIKE '^(WELCOME5|BUNDLE)$' THEN 'PRIORITY' ELSE 'CORE' END AS promo_code_zone
FROM with_windows
ORDER BY discount_pct DESC NULLS FIRST, promo_code_zone DESC
