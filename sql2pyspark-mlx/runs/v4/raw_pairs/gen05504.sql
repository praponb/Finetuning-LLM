WITH base AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        UPPER(TRIM(store_name)) AS store_name_normalized,
        UPPER(TRIM(store_name)) AS store_name_normalized_2,
        COALESCE(region, 'N/A') AS resolved_region
    FROM stores
    WHERE opened_date >= DATE '2024-02-01'
      AND region IS NOT NULL
),
latest_match AS (
    SELECT s.*, x.inventory_last_restock
    FROM base s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    SELECT
        *,
        LAG(sqft, 1) OVER w_hist AS prev_sqft,
        ROW_NUMBER() OVER (ORDER BY opened_date ASC, id ASC) AS event_seq,
        SUM(sqft) OVER w_part AS cumulative_sqft,
        NTILE(3) OVER (ORDER BY sqft ASC, id ASC) AS sqft_bucket,
        NTILE(3) OVER w_recent AS sqft_bucket_2
    FROM latest_match
    WINDOW w_hist AS (PARTITION BY region, city ORDER BY sqft ASC, id ASC), w_part AS (PARTITION BY city ORDER BY inventory_last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY city ORDER BY sqft ASC, id ASC)
)
SELECT
    id,
    city,
    region,
    opened_date,
    sqft,
    store_name_normalized,
    store_name_normalized_2,
    inventory_last_restock,
    event_seq
FROM enriched
ORDER BY opened_date DESC NULLS LAST
