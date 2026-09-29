WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        COALESCE(cost, 0) AS cost_filled,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM products
    WHERE launch_date > TO_DATE('2023-11-06')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        category,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        cost_filled,
        resolved_brand,
        LAG(cost_filled, 1) OVER (PARTITION BY category ORDER BY cost_filled ASC, id ASC) AS prev_cost_filled,
        LEAD(cost, 1, 0) OVER (PARTITION BY supplier_id, brand ORDER BY cost ASC, id ASC) AS next_cost,
        SUM(stock_qty) OVER w_part AS cumulative_stock_qty,
        FIRST_VALUE(brand, TRUE) OVER w_grp AS first_known_brand
    FROM valid
    WINDOW w_part AS (PARTITION BY category ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY brand ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT brand, id
    FROM enriched
    EXCEPT ALL
    SELECT brand, id
    FROM products
    WHERE UPPER(product_name) LIKE '%MAR%'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.brand = k.brand AND s.id = k.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(cost_filled) OVER (PARTITION BY supplier_id, brand) AS supplier_id_stddev_samp_cost_filled,
        SUM(cost) OVER (PARTITION BY supplier_id, brand) AS supplier_id_sum_cost,
        PERCENT_RANK() OVER (PARTITION BY supplier_id ORDER BY prev_cost_filled DESC) AS prev_cost_filled_percent_rank,
        CUME_DIST() OVER (ORDER BY cost_filled DESC) AS cost_filled_cume_dist
    FROM verified
),
enriched_products AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        s.country,
        s.city
    FROM with_windows e
    JOIN suppliers s
        ON e.supplier_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        category,
        cost_filled_cume_dist,
        cost_filled
    FROM enriched_products
) src
UNPIVOT (
    metric_value FOR measure IN (cost_filled_cume_dist AS COST_FILLED_CUME_DIST, cost_filled AS COST_FILLED)
)
