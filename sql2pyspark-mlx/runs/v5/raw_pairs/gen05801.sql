WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS row_seq
    FROM stores
    WHERE city != 'Tokyo'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT city, id
    FROM with_windows
    EXCEPT
    SELECT city, id
    FROM stores
    WHERE city != 'Bangkok'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock < s.opened_date
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT *, AVG(row_seq) OVER (PARTITION BY city) AS city_avg_row_seq
    FROM eligible
)
SELECT
    id,
    region,
    opened_date,
    sqft,
    sqft_avg_2,
    CASE WHEN region RLIKE '^(South|West)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS region_zone
FROM scored;
