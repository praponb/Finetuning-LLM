WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-09-14')
      AND revenue BETWEEN 1100 AND 2600
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT product_id, store_id
    FROM cleaned
    INTERSECT
    SELECT product_id, store_id
    FROM store_sales
    WHERE product_id IN (5, 8, 9)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        store_id,
        product_id,
        ROUND(units / SUM(units) OVER (PARTITION BY product_id, store_id), 4) AS units_share,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY units DESC) AS units_dense_rank
    FROM verified
    WHERE units IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        product_id,
        units_share,
        RANK() OVER w_grp AS units_dense_rank_rank,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units_dense_rank DESC NULLS FIRST) AS units_dense_rank_percent_rank,
        MAX(units_share) OVER (PARTITION BY store_id) AS store_id_max_units_share
    FROM scored
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY units_dense_rank NULLS LAST)
),
with_windows_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        product_id,
        units_share,
        units_dense_rank_rank,
        units_dense_rank_percent_rank,
        store_id_max_units_share,
        DENSE_RANK() OVER w_grp AS store_id_max_units_share_dense_rank
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY store_id_max_units_share DESC)
)
-- Join a dimension and aggregate
SELECT
    p.category,
    COUNT(*) AS cnt,
    MIN(e.store_id_max_units_share_dense_rank) AS min_store_id_max_units_share_dense_rank
FROM with_windows_store_sales e
LEFT JOIN products p ON e.product_id = p.id
GROUP BY ROLLUP (p.category)
ORDER BY min_store_id_max_units_share_dense_rank NULLS FIRST
