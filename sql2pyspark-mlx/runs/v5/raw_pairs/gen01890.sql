WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        quantity,
        ROUND(quantity, 0) AS quantity_rounded
    FROM inventory
    WHERE last_restock > '2024-09-26'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT id, store_id
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_rounded DESC, id ASC) = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        d.store_name,
        d.city,
        d.sqft
    FROM deduped e
    LEFT JOIN stores d
        ON e.store_id = d.id
    WHERE d.city = 'Tokyo'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        city,
        SUBSTRING(city, 1, 2) AS city_prefix
    FROM joined
    WHERE city = 'Chiang Mai'
),
with_dims AS (
    -- Attach stores attributes
    SELECT a.*, s.city AS store_city
    FROM base a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', city, store_city) AS city_store_city_key,
        CASE WHEN store_city IN ('Chiang Mai', 'Singapore') THEN 'Core' ELSE 'Other' END AS store_city_group,
        COALESCE(store_city, 'DIRECT') AS resolved_store_city
    FROM with_dims
    WHERE city IS NOT NULL
      AND city IS NOT NULL
),
summary AS (
    -- Multi-level aggregation
    SELECT
        city,
        COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count,
        COUNT_IF(store_city = 'New York') AS new_york_events,
        SIZE(COLLECT_SET(store_city)) AS num_store_cities,
        COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows,
        COUNT(*) AS event_count,
        SORT_ARRAY(COLLECT_SET(store_city)) AS store_city_list
    FROM valid
    GROUP BY city
)
SELECT
    city,
    tokyo_count,
    new_york_events,
    num_store_cities,
    berlin_rows,
    event_count,
    store_city_list
FROM summary
ORDER BY berlin_rows DESC NULLS LAST
