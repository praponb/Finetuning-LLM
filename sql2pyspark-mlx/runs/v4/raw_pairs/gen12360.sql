WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM promotions
    EXCEPT
    SELECT id
    FROM promotions
    WHERE promo_code IN ('WELCOME5', 'SPRING10')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    channel,
    promo_code,
    ends_on,
    CASE WHEN promo_code RLIKE '^(BUNDLE|SPRING10)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS promo_code_zone
FROM kept_rows
ORDER BY channel NULLS LAST
