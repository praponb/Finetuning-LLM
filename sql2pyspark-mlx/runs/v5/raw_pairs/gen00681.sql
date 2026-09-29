WITH latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.opened_date,
        top1.inventory_quantity,
        top1.inventory_last_restock
    FROM stores s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        region,
        opened_date,
        inventory_last_restock,
        MAX(inventory_quantity) OVER (PARTITION BY region ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_inventory_quantity,
        COUNT(inventory_quantity) OVER (PARTITION BY region ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_inventory_quantity,
        SUM(inventory_quantity) OVER (PARTITION BY region ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_inventory_quantity
    FROM latest_match
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        region,
        opened_date,
        last4_max_inventory_quantity,
        last4_sum_inventory_quantity,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY last4_sum_inventory_quantity ASC NULLS FIRST) AS last4_sum_inventory_quantity_dense_rank,
        SUM(last4_count_inventory_quantity) OVER (PARTITION BY region ORDER BY last4_max_inventory_quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last4_count_inventory_quantity,
        COUNT(last4_max_inventory_quantity) OVER (PARTITION BY region) AS region_count_last4_max_inventory_quantity
    FROM with_windows
    WHERE last4_count_inventory_quantity IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(last4_max_inventory_quantity) OVER (PARTITION BY region) AS region_stddev_samp_last4_max_inventory_quantity,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY running_last4_count_inventory_quantity ASC, id ASC) AS row_seq,
        NTILE(5) OVER (PARTITION BY region ORDER BY last4_max_inventory_quantity DESC, id ASC) AS last4_max_inventory_quantity_bucket
    FROM enriched
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.opened_date,
        s.last4_max_inventory_quantity,
        s.last4_sum_inventory_quantity,
        s.running_last4_count_inventory_quantity,
        s.region_count_last4_max_inventory_quantity,
        s.last4_max_inventory_quantity_bucket,
        latest.units,
        latest.sale_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.units, c.sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.opened_date,
        s.last4_max_inventory_quantity,
        s.last4_sum_inventory_quantity,
        s.running_last4_count_inventory_quantity,
        s.last4_max_inventory_quantity_bucket,
        s.units,
        s.sale_date,
        x.inventory_quantity
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    region,
    metric_name,
    val
FROM (
    SELECT
        id,
        region,
        last4_max_inventory_quantity,
        last4_sum_inventory_quantity
    FROM with_last
) src
UNPIVOT (
    val FOR metric_name IN (last4_max_inventory_quantity, last4_sum_inventory_quantity)
)
