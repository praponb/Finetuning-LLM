WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
    FROM inventory
    WHERE store_id IN (1, 2, 3, 5)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(quantity, 1) OVER (PARTITION BY store_id ORDER BY quantity, id) AS prev_quantity,
        MAX(quantity) OVER (PARTITION BY store_id) AS store_id_max_quantity,
        LEAD(quantity, 1, 0) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS next_quantity
    FROM valid
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.quantity,
        s.prev_quantity,
        s.store_id_max_quantity,
        d.region,
        d.store_name
    FROM with_windows s
    INNER JOIN stores d
        ON s.store_id = d.id
    WHERE d.store_name LIKE '%Mall%'
),
enriched_inventory AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(prev_quantity) OVER w_part AS running_sum_prev_quantity,
        MIN(prev_quantity) OVER w_part AS running_min_prev_quantity,
        AVG(prev_quantity) OVER w_part AS running_avg_prev_quantity,
        COUNT(prev_quantity) OVER w_part AS running_count_prev_quantity
    FROM enriched
    WINDOW w_part AS (PARTITION BY region, store_id ORDER BY store_id_max_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_inventory_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id_max_quantity,
        region,
        running_avg_prev_quantity,
        running_count_prev_quantity,
        SUM(running_sum_prev_quantity) OVER (PARTITION BY region, store_id ORDER BY running_count_prev_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_sum_prev_quantity,
        AVG(running_avg_prev_quantity) OVER (PARTITION BY region ORDER BY store_id_max_quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_avg_prev_quantity_avg_3
    FROM enriched_inventory
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched_inventory_2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY running_avg_prev_quantity_avg_3 NULLS FIRST, id) = 1
)
SELECT
    id,
    store_id_max_quantity,
    running_avg_prev_quantity,
    cumulative_running_sum_prev_quantity,
    running_avg_prev_quantity_avg_3
FROM deduped;
