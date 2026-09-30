WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        AVG(quantity) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quantity_bucket,
        MIN(quantity) OVER (PARTITION BY store_id) AS store_id_min_quantity,
        SUM(quantity) OVER (PARTITION BY store_id) AS store_id_sum_quantity
    FROM inventory
    WHERE product_id IN (1, 3, 7, 10)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        quantity_avg_2,
        ROUND(store_id_sum_quantity, 0) AS store_id_sum_quantity_rounded,
        ROUND(quantity_bucket, 0) AS quantity_bucket_rounded
    FROM enriched
    WHERE last_restock >= DATE '2023-03-17'
      AND quantity_avg_2 >= 219
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS quantity_avg_3,
        AVG(quantity_avg_2) OVER (PARTITION BY product_id ORDER BY quantity_bucket_rounded, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2_avg_2,
        AVG(quantity_avg_2) OVER (PARTITION BY product_id ORDER BY quantity_avg_2, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_2_avg_3_prior,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY store_id_sum_quantity_rounded, id) AS store_id_sum_quantity_rounded_bucket,
        SUM(quantity_avg_2) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_avg_2
    FROM cleaned
),
joined AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.quantity,
        s.last_restock,
        s.quantity_avg_2,
        s.store_id_sum_quantity_rounded,
        s.quantity_bucket_rounded,
        s.quantity_avg_2_avg_2,
        s.quantity_avg_2_avg_3_prior,
        s.running_quantity_avg_2,
        d.stock_qty
    FROM enriched_inventory s
    LEFT JOIN products d
        ON s.product_id = d.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        last_restock,
        quantity_avg_2,
        store_id_sum_quantity_rounded,
        quantity_bucket_rounded,
        quantity_avg_2_avg_3_prior,
        stock_qty,
        running_quantity_avg_2 * quantity_avg_2_avg_2 AS running_quantity_avg_2_times_quantity_avg_2_avg_2
    FROM joined
    WHERE quantity_avg_2_avg_3_prior <= 200
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM valid
    INTERSECT
    SELECT id
    FROM inventory
    WHERE store_id IN (2, 5, 6)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    ROUND(AVG(quantity_avg_2_avg_3_prior), 2) AS avg_quantity_avg_2_avg_3_prior,
    ROUND(AVG(running_quantity_avg_2_times_quantity_avg_2_avg_2), 2) AS avg_running_quantity_avg_2_times_quantity_avg_2_avg_2,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY ROUND(AVG(quantity_avg_2_avg_3_prior), 2) DESC) AS avg_quantity_avg_2_avg_3_prior_dense_rank
FROM reconciled
GROUP BY product_id, activity_month
HAVING avg_quantity_avg_2_avg_3_prior < 1000000;
