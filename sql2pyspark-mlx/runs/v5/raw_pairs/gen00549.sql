WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(quantity, 1) quantity_rounded
    FROM inventory
    WHERE quantity BETWEEN 88 AND 124
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY quantity_rounded DESC, id) quantity_rounded_bucket,
        DENSE_RANK() OVER (PARTITION BY product_id, store_id ORDER BY quantity_rounded DESC NULLS LAST) quantity_rounded_dense_rank
    FROM valid
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(quantity_rounded_dense_rank) OVER (PARTITION BY product_id ORDER BY quantity_rounded_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_quantity_rounded_dense_rank,
        MAX(quantity_rounded_dense_rank) OVER (PARTITION BY product_id ORDER BY quantity_rounded_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_quantity_rounded_dense_rank,
        COUNT(quantity_rounded_dense_rank) OVER (PARTITION BY product_id ORDER BY quantity_rounded_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_quantity_rounded_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY quantity_rounded_dense_rank) quantity_rounded_dense_rank_percent_rank
    FROM enriched
    WHERE product_id = 8
),
enriched_inventory_2 AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.running_min_quantity_rounded_dense_rank,
        s.running_max_quantity_rounded_dense_rank,
        s.running_count_quantity_rounded_dense_rank,
        s.quantity_rounded_dense_rank_percent_rank,
        p.product_name,
        p.stock_qty,
        p.launch_date
    FROM enriched_inventory s
    LEFT JOIN products p
        ON s.product_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(running_min_quantity_rounded_dense_rank) OVER (PARTITION BY product_id ORDER BY running_max_quantity_rounded_dense_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_running_min_quantity_rounded_dense_rank,
        MIN(running_min_quantity_rounded_dense_rank) OVER (PARTITION BY product_id ORDER BY running_max_quantity_rounded_dense_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_running_min_quantity_rounded_dense_rank,
        AVG(running_min_quantity_rounded_dense_rank) OVER (PARTITION BY product_id ORDER BY running_max_quantity_rounded_dense_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_running_min_quantity_rounded_dense_rank
    FROM enriched_inventory_2
)
SELECT
    id,
    store_id,
    product_id,
    running_min_quantity_rounded_dense_rank,
    running_max_quantity_rounded_dense_rank,
    quantity_rounded_dense_rank_percent_rank,
    product_name,
    stock_qty,
    last3_min_running_min_quantity_rounded_dense_rank,
    CASE WHEN product_name RLIKE '^(Desk Lamp|Running Shoes)$' THEN 'GROUP_B' WHEN product_name RLIKE '^(Laptop Pro 14|wireless mouse)$' THEN 'CORE' ELSE 'PRIORITY' END AS product_name_zone
FROM scored;
