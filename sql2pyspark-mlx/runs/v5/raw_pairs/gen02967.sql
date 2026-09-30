WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        ROUND(quantity * 0.85, 2) adjusted_quantity,
        CASE WHEN quantity >= 253 THEN 'High' WHEN quantity >= 70 THEN 'Medium' ELSE 'Low' END quantity_level
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-08-10')
),
remaining_keys AS (
    SELECT id
    FROM valid
    EXCEPT DISTINCT
    SELECT id
    FROM inventory
    WHERE last_restock <= '2023-06-01'
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.id = k.id
),
valid_inventory AS (
    SELECT
        *,
        CASE WHEN adjusted_quantity >= 87 THEN 'High' WHEN adjusted_quantity >= 15 THEN 'Medium' ELSE 'Low' END adjusted_quantity_tier,
        FLOOR(quantity) quantity_whole
    FROM kept_rows
    WHERE last_restock <= '2025-10-01'
),
metrics AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY product_id, store_id ORDER BY quantity_whole DESC) quantity_whole_rank,
        CUME_DIST() OVER w_part quantity_whole_cume_dist,
        STDDEV_SAMP(quantity) OVER w_grp product_id_stddev_samp_quantity
    FROM valid_inventory
    WHERE quantity BETWEEN 142 AND 233
    WINDOW w_part AS (PARTITION BY product_id, store_id ORDER BY quantity_whole DESC NULLS LAST), w_grp AS (PARTITION BY product_id)
),
cleaned AS (
    SELECT
        id,
        store_id,
        quantity,
        adjusted_quantity,
        quantity_level,
        adjusted_quantity_tier,
        quantity_whole,
        quantity_whole_rank,
        quantity_whole_cume_dist,
        CAST(quantity AS DOUBLE) quantity_num,
        quantity_whole_cume_dist * adjusted_quantity quantity_whole_cume_dist_times_adjusted_quantity
    FROM metrics
    WHERE last_restock >= DATE '2023-06-28'
),
with_windows AS (
    SELECT *, CUME_DIST() OVER (PARTITION BY store_id ORDER BY quantity_whole NULLS LAST) quantity_whole_cume_dist_2
    FROM cleaned
),
with_dims AS (
    SELECT
        src.id,
        src.store_id,
        src.quantity_level,
        src.adjusted_quantity_tier,
        src.quantity_whole,
        src.quantity_whole_cume_dist_times_adjusted_quantity,
        src.quantity_whole_cume_dist_2,
        p.sqft
    FROM with_windows src
    LEFT JOIN stores p
        ON src.store_id = p.id
)
SELECT
    CASE WHEN quantity_whole >= 90 THEN 'LARGE' ELSE 'SMALL' END AS quantity_whole_tier,
    store_id,
    CASE WHEN quantity_whole_cume_dist_times_adjusted_quantity >= 71 THEN 'LARGE' ELSE 'SMALL' END AS quantity_whole_cume_dist_times_adjusted_quantity_tier,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    COUNT(DISTINCT store_id) AS distinct_store_ids_2,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_dims
GROUP BY CASE WHEN quantity_whole >= 90 THEN 'LARGE' ELSE 'SMALL' END, store_id, CASE WHEN quantity_whole_cume_dist_times_adjusted_quantity >= 71 THEN 'LARGE' ELSE 'SMALL' END;
