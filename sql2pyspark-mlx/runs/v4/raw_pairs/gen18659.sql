WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock >= DATE '2024-04-16'
      AND last_restock BETWEEN '2023-07-01' AND '2023-11-01'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_filled,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id ORDER BY quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        NTILE(10) OVER (ORDER BY quantity_filled, id) AS quantity_filled_bucket
    FROM valid
),
with_dims AS (
    -- Attach stores attributes
    SELECT s.*, b.store_name
    FROM enriched s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM with_dims
    EXCEPT ALL
    SELECT store_id, id
    FROM inventory
    WHERE store_id IN (1, 4)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN remaining_keys k ON s.store_id = k.store_id AND s.id = k.id
),
joined AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.sqft,
        b.opened_date,
        b.city
    FROM kept_rows a
    LEFT JOIN stores b
        ON a.store_id = b.id
)
-- Grouped metrics
SELECT
    CASE WHEN quantity_filled >= 78 THEN 'LARGE' ELSE 'SMALL' END AS quantity_filled_tier,
    CASE WHEN quantity_filled_bucket >= 6 THEN 'LARGE' ELSE 'SMALL' END AS quantity_filled_bucket_tier,
    ROUND(SUM(quantity) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS quantity_per_singapore,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SUM(CASE WHEN city = 'Tokyo' THEN quantity_filled ELSE 0 END) AS tokyo_quantity_filled
FROM joined
GROUP BY 1, 2
