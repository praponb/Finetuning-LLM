WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        opened_date,
        DATE_ADD(opened_date, 85) AS opened_date_plus_85d,
        UPPER(TRIM(store_name)) AS store_name_normalized
    FROM stores
    WHERE region IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.shipping_fee > 27
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        opened_date,
        opened_date_plus_85d,
        store_name_normalized,
        REGEXP_REPLACE(store_name, ' ', '_') AS store_name_slug,
        LAST_DAY(opened_date_plus_85d) AS opened_date_plus_85d_month_end
    FROM eligible
    WHERE opened_date_plus_85d BETWEEN '2024-04-01' AND '2025-07-12'
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_last_restock,
        top1.quantity
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_last
    EXCEPT ALL
    SELECT id
    FROM stores
    WHERE sqft BETWEEN 7574 AND 12558
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN remaining_keys k ON s.id = k.id
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', opened_date_plus_85d_month_end) AS year_start,
        PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
        APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles_2,
        APPROX_PERCENTILE(quantity, 0.5) AS quantity_p50,
        PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles_3
    FROM reconciled
    GROUP BY DATE_TRUNC('YEAR', opened_date_plus_85d_month_end)
)
SELECT
    year_start,
    quantity_quartiles,
    quantity_quartiles_2,
    quantity_quartiles_3
FROM daily
ORDER BY year_start DESC NULLS LAST
