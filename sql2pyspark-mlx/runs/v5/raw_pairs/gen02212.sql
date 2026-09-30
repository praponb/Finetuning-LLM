WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        COALESCE(region, 'N/A') AS region_filled,
        UPPER(TRIM(store_name)) AS store_name_normalized
    FROM stores
    WHERE region = 'Central'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.product_id = 6
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.amount BETWEEN 760 AND 860
        )
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.region,
        s.sqft,
        s.region_filled,
        s.store_name_normalized,
        top1.store_sale_units
    FROM eligible s
    JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest_stores AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.quantity,
        x.last_restock
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY store_sale_units, id) AS event_seq,
        ROUND(store_sale_units / SUM(store_sale_units) OVER (PARTITION BY region), 4) AS store_sale_units_share,
        LEAD(sqft, 2) OVER (PARTITION BY region ORDER BY store_sale_units, id) AS next_sqft
    FROM with_latest_stores
)
SELECT
    id,
    store_name,
    region,
    region_filled,
    last_restock,
    event_seq,
    store_sale_units_share
FROM with_windows
ORDER BY last_restock DESC;
