WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_ADD(opened_date, 73) AS opened_date_plus_73d
    FROM stores
    WHERE opened_date > TO_DATE('2023-04-01')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock < s.opened_date_plus_73d
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date_plus_73d NULLS FIRST, id) = 1
)
SELECT
    id,
    city,
    region,
    opened_date,
    sqft,
    opened_date_plus_73d,
    CASE WHEN region RLIKE '^(North|Central)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS region_zone
FROM latest
