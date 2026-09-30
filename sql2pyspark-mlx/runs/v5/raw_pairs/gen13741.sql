WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DAY(last_restock) AS last_restock_day,
        CAST(quantity AS STRING) AS quantity_str
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-07-01')
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id, id
    FROM filtered
    INTERSECT
    SELECT store_id, id
    FROM inventory
    WHERE last_restock BETWEEN '2023-07-20' AND '2025-08-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.store_id = k.store_id AND s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        store_id,
        quantity,
        last_restock_day,
        quantity_str,
        COUNT(quantity) OVER (PARTITION BY product_id) AS product_id_count_quantity,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST) AS quantity_percent_rank
    FROM reconciled
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p.region,
        p.sqft,
        p.store_name
    FROM with_windows s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
with_windows_inventory AS (
    -- Running and moving aggregates
    SELECT
        last_restock_day,
        product_id_count_quantity,
        region,
        CUME_DIST() OVER (PARTITION BY region ORDER BY product_id_count_quantity NULLS FIRST) AS product_id_count_quantity_cume_dist,
        ROUND(product_id_count_quantity / SUM(product_id_count_quantity) OVER (PARTITION BY region), 4) AS product_id_count_quantity_share,
        CUME_DIST() OVER (PARTITION BY region ORDER BY sqft NULLS FIRST) AS sqft_cume_dist,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY store_id), 4) AS sqft_share
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        region,
        sqft_share,
        sqft_cume_dist,
        product_id_count_quantity_share,
        product_id_count_quantity_cume_dist
    FROM with_windows_inventory
) src
UNPIVOT (
    metric_value FOR metric_name IN (sqft_share AS SQFT_SHARE, sqft_cume_dist AS SQFT_CUME_DIST, product_id_count_quantity_share, product_id_count_quantity_cume_dist AS PRODUCT_ID_COUNT_QUANTITY_CUME_DIST)
)
