WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity,
        last_restock,
        LEAD(quantity, 2) OVER w_ordered AS next_quantity,
        LAG(quantity, 2) OVER w_ordered_2 AS prev_quantity,
        AVG(quantity) OVER w_grp AS quantity_avg_5,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        NTILE(4) OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) AS quantity_bucket
    FROM inventory
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY last_restock, id), w_ordered_2 AS (ORDER BY quantity, id), w_grp AS (ORDER BY last_restock, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched
    INTERSECT ALL
    SELECT id
    FROM inventory
    WHERE store_id IN (1, 4)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.id = k.id
),
matched_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM verified
    EXCEPT ALL
    SELECT id
    FROM inventory
    WHERE product_id IN (1, 3, 8)
),
verified_inventory AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN matched_keys_2 k ON s.id = k.id
),
enriched_inventory AS (
    -- Ranking and gap features
    SELECT
        quantity,
        prev_quantity,
        RANK() OVER w_ordered AS quantity_bucket_rank,
        PERCENT_RANK() OVER (ORDER BY quantity_bucket DESC) AS quantity_bucket_percent_rank
    FROM verified_inventory
    WHERE prev_quantity < 120
    WINDOW w_ordered AS (ORDER BY quantity_bucket DESC NULLS LAST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN quantity >= 203 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    PERCENTILE_APPROX(quantity_bucket_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS quantity_bucket_percent_rank_quartiles,
    ROUND(AVG(prev_quantity), 2) AS avg_prev_quantity,
    ROUND(AVG(quantity_bucket_percent_rank), 2) AS avg_quantity_bucket_percent_rank,
    BIT_AND(quantity) AS quantity_bit_and,
    SUM(quantity_bucket_percent_rank) AS total_quantity_bucket_percent_rank
FROM enriched_inventory
GROUP BY CASE WHEN quantity >= 203 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(AVG(prev_quantity), 2) < 1000000
ORDER BY quantity_bit_and NULLS LAST
