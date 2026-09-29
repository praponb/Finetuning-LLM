WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE last_restock > DATE '2023-09-26'
      AND YEAR(last_restock) >= 2024
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT id, store_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock_month_end NULLS LAST, id) AS rank_in_group
        FROM cleaned
    ) x
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        b.region,
        b.city
    FROM deduped s
    INNER JOIN stores b
        ON s.store_id = b.id
),
enriched_inventory AS (
    -- Attach stores attributes
    SELECT e.*, b.city AS store_city
    FROM enriched e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
cleaned_inventory AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(region) AS region_upper,
        LOWER(city) AS city_lower
    FROM enriched_inventory
    WHERE store_city IS NOT NULL
      AND store_id IN (1, 2, 3)
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.region,
        src.city_lower,
        p.store_name
    FROM cleaned_inventory src
    INNER JOIN stores p
        ON src.store_id = p.id
)
-- Multi-level aggregation
SELECT
    region,
    store_id,
    SIZE(COLLECT_SET(region)) AS num_regions,
    SIZE(COLLECT_SET(region)) AS num_regions_2,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    COUNT(CASE WHEN region = 'West' THEN 1 END) AS west_count
FROM with_dims
GROUP BY region, store_id;
