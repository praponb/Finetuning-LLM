WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock NULLS LAST, id) AS row_num
        FROM inventory
    ) t
    WHERE row_num = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY quantity, id) AS quantity_bucket,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM deduped
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_bucket DESC NULLS LAST, id) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        cumulative_quantity,
        PERCENT_RANK() OVER (ORDER BY cumulative_quantity DESC) AS cumulative_quantity_percent_rank,
        AVG(cumulative_quantity) OVER (PARTITION BY product_id) AS product_id_avg_cumulative_quantity,
        NTILE(3) OVER (ORDER BY quantity DESC, id) AS quantity_bucket_2
    FROM latest
    WHERE row_num >= 1
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.product_id_avg_cumulative_quantity,
        p.cost
    FROM enriched e
    LEFT JOIN products p
        ON e.product_id = p.id
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(product_id_avg_cumulative_quantity) OVER w_grp AS cumulative_product_id_avg_cumulative_quantity,
        CUME_DIST() OVER (PARTITION BY product_id, store_id ORDER BY cost NULLS FIRST) AS cost_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cost, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id_avg_cumulative_quantity DESC, id) AS row_seq
    FROM joined
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_product_id_avg_cumulative_quantity >= 5852 THEN 'HIGH' ELSE 'LOW' END AS cumulative_product_id_avg_cumulative_quantity_tier,
    CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched_inventory
GROUP BY CASE WHEN cumulative_product_id_avg_cumulative_quantity >= 5852 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END
ORDER BY seq_no_tier
