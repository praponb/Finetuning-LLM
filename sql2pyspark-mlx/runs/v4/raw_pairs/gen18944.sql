WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        ROUND(sqft, 1) AS sqft_rounded,
        COALESCE(region, 'unassigned') AS region_filled
    FROM stores
    WHERE opened_date > TO_DATE('2023-04-19')
      AND region IS NOT NULL
      AND sqft BETWEEN 9700 AND 15409
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (ORDER BY sqft_rounded, id) AS sqft_rounded_bucket,
        LAG(sqft_rounded, 1) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft_rounded,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft_rounded DESC) AS sqft_rounded_dense_rank,
        LEAD(sqft_rounded, 2) OVER (ORDER BY sqft_rounded, id) AS next_sqft_rounded
    FROM valid
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.quantity,
        x.inventory_last_restock
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_last
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY next_sqft_rounded DESC, id) = 1
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft_rounded,
    prev_sqft_rounded,
    quantity,
    inventory_last_restock,
    CASE WHEN city RLIKE '^(Berlin|Bangkok)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS city_zone
FROM latest
ORDER BY prev_sqft_rounded DESC NULLS FIRST
