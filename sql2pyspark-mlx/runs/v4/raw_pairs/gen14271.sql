WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        COALESCE(quantity, 0) quantity_filled,
        YEAR(last_restock) last_restock_year
    FROM inventory
    WHERE last_restock > TO_DATE('2023-09-01')
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        p.sqft,
        p.region
    FROM valid a
    LEFT JOIN stores p
        ON a.store_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(last_restock_year, TRUE) OVER (ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_last_restock_year,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY last_restock_year ASC, id ASC) row_seq,
        NTILE(10) OVER (PARTITION BY store_id ORDER BY quantity_filled ASC, id ASC) quantity_filled_bucket,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region, store_id), 4) sqft_share,
        LAG(quantity_filled, 1) OVER (PARTITION BY store_id, region ORDER BY last_restock_year ASC, id ASC) prev_quantity_filled
    FROM with_dims
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY last_restock ASC) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM latest
    EXCEPT
    SELECT store_id, id
    FROM inventory
    WHERE last_restock BETWEEN '2024-06-01' AND '2025-04-07'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        last_restock_year,
        sqft,
        region,
        last_known_last_restock_year,
        row_seq,
        quantity_filled_bucket,
        sqft_share,
        prev_quantity_filled,
        dedup_rank,
        COALESCE(quantity, 0) quantity_filled_2,
        COALESCE(region, 'N/A') region_filled
    FROM reconciled
    WHERE last_restock > DATE '2023-06-15'
      AND region IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY region ORDER BY quantity DESC) quantity_cume_dist,
        CUME_DIST() OVER (ORDER BY prev_quantity_filled ASC NULLS FIRST) prev_quantity_filled_cume_dist,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) quantity_dense_rank,
        MIN(last_known_last_restock_year) OVER (PARTITION BY store_id) store_id_min_last_known_last_restock_year
    FROM filtered
)
-- Grouped metrics
SELECT
    CASE WHEN quantity_filled_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_filled_bucket_tier,
    BIT_OR(quantity) AS quantity_bit_or,
    ROUND(AVG(quantity_filled_bucket), 2) AS avg_quantity_filled_bucket,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list
FROM with_windows
GROUP BY CASE WHEN quantity_filled_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY quantity_filled_bucket_tier DESC NULLS LAST;
