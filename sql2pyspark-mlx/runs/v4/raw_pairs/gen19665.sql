WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('/', city, store_name) city_store_name_key,
        COALESCE(region, 'UNKNOWN') region_filled
    FROM stores
    WHERE opened_date > TO_DATE('2023-06-06')
      AND region IS NOT NULL
      AND city = 'Berlin'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft NULLS LAST, id) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_last_restock,
        top1.quantity
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT region, city
    FROM with_latest
    INTERSECT ALL
    SELECT region, city
    FROM stores
    WHERE sqft < 7592
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    JOIN remaining_keys k ON s.region = k.region AND s.city = k.city
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN quantity >= 267 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
        SIZE(COLLECT_SET(region)) AS num_regions,
        PERCENTILE_APPROX(sqft, 0.5) AS sqft_p50,
        SUM(CASE WHEN region = 'South' THEN quantity ELSE 0 END) AS south_quantity
    FROM kept_rows
    GROUP BY 1
    HAVING PERCENTILE_APPROX(sqft, 0.5) < 0
)
SELECT
    quantity_tier,
    num_regions,
    sqft_p50,
    south_quantity
FROM daily
ORDER BY num_regions DESC;
