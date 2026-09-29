WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY quantity DESC NULLS FIRST) quantity_cume_dist,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) quantity_rank,
        SUM(quantity) OVER (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity,
        SUM(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity_2
    FROM inventory
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_cume_dist DESC NULLS FIRST, id) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT store_id
    FROM latest
    EXCEPT
    SELECT store_id
    FROM inventory
    WHERE last_restock > '2024-11-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.store_id = k.store_id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        p.opened_date,
        p.region,
        p.sqft
    FROM kept_rows e
    JOIN stores p
        ON e.store_id = p.id
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.stock_qty,
        p.product_name
    FROM with_dims s
    LEFT JOIN products p
        ON s.product_id = p.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN quantity_cume_dist >= 0.6 THEN 'LARGE' ELSE 'SMALL' END AS quantity_cume_dist_tier,
        DATE_TRUNC('YEAR', opened_date) AS year_start,
        region,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list,
        SIZE(COLLECT_SET(region)) AS num_regions,
        COUNT(CASE WHEN region = 'North' THEN 1 END) AS north_count
    FROM joined
    GROUP BY CASE WHEN quantity_cume_dist >= 0.6 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', opened_date), region
    HAVING num_regions < 1
)
SELECT
    quantity_cume_dist_tier,
    year_start,
    region,
    region_list,
    num_regions,
    north_count
FROM daily
ORDER BY region DESC;
