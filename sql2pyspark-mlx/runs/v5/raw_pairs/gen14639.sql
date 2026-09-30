WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.last_restock,
        s.store_name,
        s.sqft
    FROM inventory a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.opened_date,
        p2.sqft AS store_sqft,
        p2.store_name AS store_store_name
    FROM with_dims s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
enriched_inventory AS (
    -- Attach stores attributes
    SELECT s.*, p.city
    FROM enriched s
    INNER JOIN stores p
        ON s.store_id = p.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(store_name) AS store_name_lower
    FROM enriched_inventory
    WHERE city IS NOT NULL
      AND sqft = 7072
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id, id
    FROM filtered
    INTERSECT ALL
    SELECT store_id, id
    FROM inventory
    WHERE quantity IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id AND s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER w_ordered AS sqft_cume_dist
    FROM verified
    WHERE store_id IN (2, 3, 4, 5)
    WINDOW w_ordered AS (ORDER BY sqft NULLS FIRST)
),
metrics AS (
    -- Running and moving aggregates
    SELECT *, RANK() OVER (PARTITION BY city, store_id ORDER BY store_sqft DESC NULLS LAST) AS store_sqft_rank
    FROM with_windows
)
SELECT
    id,
    store_id,
    store_name,
    sqft,
    store_sqft,
    store_store_name,
    city,
    store_name_lower,
    sqft_cume_dist
FROM metrics
ORDER BY id DESC NULLS FIRST
