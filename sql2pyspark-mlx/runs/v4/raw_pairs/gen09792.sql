WITH deduped AS (
    SELECT *, RANK() OVER (PARTITION BY city, region ORDER BY opened_date) AS row_num
    FROM stores
    QUALIFY row_num = 1
),
kept AS (
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date <= s.opened_date
        )
),
kept_keys AS (
    SELECT region, city
    FROM kept
    EXCEPT
    SELECT region, city
    FROM stores
    WHERE sqft IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM kept s
    JOIN kept_keys k ON s.region = k.region AND s.city = k.city
)
SELECT
    id,
    store_name,
    city,
    opened_date,
    sqft,
    row_num,
    CASE WHEN store_name RLIKE '^(Airport Kiosk|Downtown Express)$' THEN 'EXTENDED' WHEN store_name RLIKE '^(Siam Flagship|Mall Central)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS store_name_zone
FROM verified
ORDER BY store_name_zone DESC
