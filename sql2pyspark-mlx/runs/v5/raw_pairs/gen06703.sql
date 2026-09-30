WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(sqft * 0.85, 2) AS adjusted_sqft,
        LENGTH(region) AS region_length,
        COALESCE(region, 'NONE') AS region_filled
    FROM stores
    WHERE opened_date > DATE '2023-02-25'
      AND region IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date >= s.opened_date
        )
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_last_restock,
        x.inventory_quantity
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(inventory_quantity) OVER (PARTITION BY city ORDER BY inventory_last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_inventory_quantity,
        MAX(inventory_quantity) OVER (PARTITION BY city ORDER BY inventory_last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_inventory_quantity,
        MIN(inventory_quantity) OVER (PARTITION BY city ORDER BY inventory_last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_inventory_quantity,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY inventory_quantity ASC, id ASC) AS event_seq
    FROM with_last
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY inventory_quantity DESC, id ASC) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
)
-- Running and moving aggregates
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    inventory_last_restock,
    inventory_quantity,
    running_count_inventory_quantity,
    running_max_inventory_quantity,
    running_min_inventory_quantity,
    event_seq,
    latest_rank,
    SUM(running_min_inventory_quantity) OVER (PARTITION BY city) AS city_sum_running_min_inventory_quantity,
    AVG(running_count_inventory_quantity) OVER (PARTITION BY city, region) AS city_avg_running_count_inventory_quantity,
    SUM(inventory_quantity) OVER (PARTITION BY region ORDER BY inventory_quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_inventory_quantity,
    AVG(inventory_quantity) OVER (PARTITION BY city, region ORDER BY latest_rank ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS inventory_quantity_avg_5_prior
FROM deduped
