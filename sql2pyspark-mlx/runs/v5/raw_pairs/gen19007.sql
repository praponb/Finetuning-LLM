WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(city)) AS city_title,
        ROUND(sqft * 1.1, 2) AS adjusted_sqft,
        COALESCE(region, 'N/A') AS region_filled
    FROM stores
    WHERE region IS DISTINCT FROM 'Central'
      AND opened_date > DATE '2023-08-19'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(region, 1, 2) AS region_prefix,
        COALESCE(region, 'UNKNOWN') AS region_filled_2
    FROM filtered
    WHERE opened_date > DATE '2023-05-26'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date <= s.opened_date
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.store_id = 17
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        adjusted_sqft,
        region_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id ASC) AS rank_in_group
        FROM eligible
    ) ranked
    WHERE rank_in_group = 1
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.inventory_quantity,
        latest.last_restock
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        region,
        city,
        inventory_quantity
    FROM latest_match
) src
PIVOT (
    AVG(inventory_quantity) AS value_sum
    FOR city IN ('Berlin' AS berlin, 'Tokyo' AS tokyo)
);
