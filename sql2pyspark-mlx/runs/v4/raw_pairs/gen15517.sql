WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATE_TRUNC('WEEK', last_restock) AS last_restock_period,
        CASE WHEN quantity >= 259 THEN 'High' WHEN quantity >= 81 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE last_restock > DATE '2024-04-03'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_period NULLS FIRST, id) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_period,
        quantity_tier,
        dedup_rank,
        ROW_NUMBER() OVER (ORDER BY dedup_rank, id) AS seq_no,
        RANK() OVER w_part AS quantity_rank
    FROM deduped
    WINDOW w_part AS (PARTITION BY product_id, store_id ORDER BY quantity DESC NULLS FIRST)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', last_restock_period) AS activity_month,
    MAX(last_restock_period) AS last_seen,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
    COUNT(*) AS num_rows,
    GROUPING(DATE_TRUNC('MONTH', last_restock_period)) AS gid
FROM enriched
GROUP BY CUBE (DATE_TRUNC('MONTH', last_restock_period))
HAVING COUNT(DISTINCT store_id) < 1
ORDER BY distinct_store_ids DESC
