WITH latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_last_restock,
        x.quantity
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(region) IGNORE NULLS OVER (PARTITION BY region ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
        AVG(quantity) OVER w_ordered AS quantity_avg_3
    FROM latest_match
    WINDOW w_ordered AS (PARTITION BY region ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY quantity DESC, id) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY rn, id) AS seq_no,
        NTILE(10) OVER (ORDER BY quantity, id) AS quantity_bucket
    FROM ranked
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.revenue <= 1300
        )
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        inventory_last_restock,
        quantity,
        last_known_region,
        quantity_avg_3,
        quantity_bucket,
        ROUND(quantity_bucket, 1) AS quantity_bucket_rounded
    FROM kept
    WHERE opened_date >= TO_DATE('2024-04-07')
      AND region IS NOT NULL
)
-- Grouped metrics
SELECT
    region,
    BIT_OR(sqft) AS sqft_bit_or,
    COUNT_IF(city = 'Chiang Mai') AS chiang_mai_events,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_AND(sqft) AS sqft_bit_and,
    AVG(quantity_avg_3) AS avg_quantity_avg_3
FROM valid
GROUP BY region
ORDER BY sqft_bit_and DESC;
