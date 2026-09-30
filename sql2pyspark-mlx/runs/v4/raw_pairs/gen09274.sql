WITH base AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CAST(quantity AS STRING) AS quantity_str
    FROM inventory
    WHERE product_id = 8
),
enriched AS (
    SELECT s.*, p.region
    FROM base s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
filtered AS (
    SELECT
        *,
        LAST_DAY(last_restock) AS last_restock_month_end,
        CASE WHEN region = 'West' THEN 1 ELSE 0 END AS is_west,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM enriched
    WHERE last_restock >= DATE '2023-05-31'
)
SELECT
    id,
    product_id,
    quantity,
    last_restock,
    quantity_str,
    region,
    last_restock_month_end,
    is_west,
    resolved_region
FROM filtered
ORDER BY quantity DESC NULLS LAST
