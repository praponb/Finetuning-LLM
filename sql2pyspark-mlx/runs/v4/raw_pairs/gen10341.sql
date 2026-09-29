WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        AVG(quantity) OVER w_grp quantity_avg_3,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity
    FROM inventory
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT product_id, store_id
    FROM scored
    EXCEPT DISTINCT
    SELECT product_id, store_id
    FROM inventory
    WHERE last_restock >= '2024-01-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
with_dims AS (
    -- Attach stores attributes
    SELECT s.*, p.city
    FROM reconciled s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_avg_3,
        cumulative_quantity,
        city,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY cumulative_quantity DESC) cumulative_quantity_dense_rank,
        DENSE_RANK() OVER (PARTITION BY product_id, city ORDER BY cumulative_quantity DESC) cumulative_quantity_dense_rank_2
    FROM with_dims
)
-- Window metrics over several different partitions
SELECT
    *,
    RANK() OVER (PARTITION BY product_id, city ORDER BY cumulative_quantity DESC) cumulative_quantity_rank,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY cumulative_quantity_dense_rank DESC) cumulative_quantity_dense_rank_dense_rank,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY cumulative_quantity_dense_rank_2) cumulative_quantity_dense_rank_2_dense_rank,
    ROUND(cumulative_quantity_dense_rank / SUM(cumulative_quantity_dense_rank) OVER (PARTITION BY city), 4) cumulative_quantity_dense_rank_share,
    ROUND(cumulative_quantity_dense_rank / SUM(cumulative_quantity_dense_rank) OVER (PARTITION BY product_id), 4) cumulative_quantity_dense_rank_share_2
FROM metrics;
