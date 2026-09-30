WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(store_name, 'N/A') store_name_filled,
        CONCAT_WS(',', city, region) city_region_key
    FROM stores
    WHERE opened_date >= DATE '2023-08-10'
      AND region IS NOT NULL
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.last_restock
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('MONTH', last_restock) last_restock_period,
        COALESCE(region, 'N/A') region_filled
    FROM with_last
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'Central'
),
filtered_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LAST_DAY(opened_date) opened_date_month_end,
        COALESCE(region, 'UNKNOWN') region_clean
    FROM filtered
    WHERE opened_date > TO_DATE('2023-02-26')
      AND region IS NOT NULL
)
SELECT
    id,
    region,
    opened_date,
    sqft,
    city_region_key,
    last_restock,
    last_restock_period,
    region_filled,
    opened_date_month_end
FROM filtered_stores
ORDER BY id NULLS LAST;
