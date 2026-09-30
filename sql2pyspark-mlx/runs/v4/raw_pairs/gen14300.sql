WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(quantity, 0) quantity_filled
    FROM inventory
    WHERE last_restock >= DATE '2023-06-18'
      AND store_id IN (1, 4)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (ORDER BY quantity DESC, id ASC) quantity_bucket,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY quantity_filled ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) quantity_avg_2
    FROM cleaned
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id, product_id
    FROM enriched
    EXCEPT ALL
    SELECT store_id, product_id
    FROM inventory
    WHERE store_id IN (1, 3, 5, 6)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(quantity) OVER (PARTITION BY store_id) store_id_stddev_samp_quantity,
        ROUND(quantity_bucket / SUM(quantity_bucket) OVER (PARTITION BY store_id), 4) quantity_bucket_share,
        SUM(quantity_avg_2) OVER w_grp product_id_sum_quantity_avg_2
    FROM reconciled
    WINDOW w_grp AS (PARTITION BY product_id, store_id)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, quantity * quantity_avg_2 quantity_times_quantity_avg_2
    FROM scored
    WHERE last_restock >= DATE '2024-03-21'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id, id
    FROM base
    INTERSECT ALL
    SELECT store_id, id
    FROM inventory
    WHERE last_restock < '2025-03-01'
),
reconciled_inventory AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id AND s.id = k.id
),
joined AS (
    -- Attach products attributes
    SELECT a.*, p.stock_qty
    FROM reconciled_inventory a
    INNER JOIN products p
        ON a.product_id = p.id
),
report AS (
    SELECT
        store_id,
        quantity,
        last_restock,
        quantity_avg_2,
        store_id_stddev_samp_quantity,
        product_id_sum_quantity_avg_2
    FROM joined
)
SELECT
    last_restock,
    store_id_stddev_samp_quantity,
    product_id_sum_quantity_avg_2
FROM report
