WITH matched_keys AS (
    -- Set intersection on keys
    SELECT channel
    FROM promotions
    INTERSECT ALL
    SELECT channel
    FROM promotions
    WHERE promo_code = 'WELCOME5'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    JOIN matched_keys k ON s.channel = k.channel
)
SELECT
    id,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    CASE WHEN promo_code RLIKE '^(VIP|BUNDLE)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS promo_code_zone
FROM kept_rows
ORDER BY promo_code NULLS LAST
