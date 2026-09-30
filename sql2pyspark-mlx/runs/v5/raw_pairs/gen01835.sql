WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        ROUND(quantity * 1.07, 2) adjusted_quantity
    FROM inventory
    WHERE product_id IN (2, 9)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        adjusted_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY adjusted_quantity DESC, id) seq_no,
        NTILE(5) OVER (ORDER BY adjusted_quantity, id) adjusted_quantity_bucket,
        SUM(quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity
    FROM valid
),
scored_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY quantity DESC, id) quantity_bucket,
        LAST_VALUE(seq_no, TRUE) OVER (PARTITION BY store_id ORDER BY running_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_seq_no
    FROM scored
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(quantity_bucket) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) quantity_bucket_avg_3,
        LAST_VALUE(adjusted_quantity, TRUE) OVER (PARTITION BY store_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_adjusted_quantity,
        LAG(seq_no, 1) OVER (PARTITION BY store_id ORDER BY last_restock, id) prev_seq_no
    FROM scored_inventory
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.seq_no,
        e.adjusted_quantity_bucket,
        e.running_quantity,
        d.region
    FROM with_windows e
    LEFT JOIN stores d
        ON e.store_id = d.id
    WHERE d.region != 'Central'
)
-- Grouped metrics
SELECT
    CASE WHEN adjusted_quantity_bucket >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_quantity_bucket_tier,
    store_id,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
    ROUND(AVG(adjusted_quantity_bucket), 2) AS avg_adjusted_quantity_bucket
FROM joined
GROUP BY ROLLUP (CASE WHEN adjusted_quantity_bucket >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END, store_id);
