WITH with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.sale_date,
        e.units,
        d.brand
    FROM store_sales e
    LEFT JOIN products d
        ON e.product_id = d.id
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        s.region,
        s.store_name,
        s.sqft
    FROM with_dims e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(sqft) OVER w_ordered next_sqft,
        LAG(units) OVER w_ordered prev_units,
        ROW_NUMBER() OVER w_ordered row_seq
    FROM joined
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY sqft ASC, id ASC)
),
joined_store_sales AS (
    -- Attach stores attributes
    SELECT
        e.*,
        p.region AS store_region,
        p.store_name AS store_store_name,
        p.sqft AS store_sqft
    FROM enriched e
    INNER JOIN stores p
        ON e.store_id = p.id
),
enriched_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(units) OVER (PARTITION BY store_region ORDER BY sqft ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_units,
        AVG(units) OVER (PARTITION BY store_region ORDER BY sqft ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_units,
        SUM(units) OVER (PARTITION BY store_region ORDER BY sqft ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_units,
        MIN(units) OVER (PARTITION BY store_region ORDER BY sqft ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_units
    FROM joined_store_sales
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        sale_date,
        units,
        region,
        sqft,
        next_sqft,
        prev_units,
        row_seq,
        store_region,
        last7_sum_units,
        MAX(last7_sum_units) OVER w_grp running_max_last7_sum_units,
        MIN(last7_sum_units) OVER w_grp running_min_last7_sum_units,
        AVG(last7_sum_units) OVER w_grp running_avg_last7_sum_units,
        SUM(last7_sum_units) OVER w_grp running_sum_last7_sum_units
    FROM enriched_store_sales
    WINDOW w_grp AS (PARTITION BY brand, region ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        store_region,
        store_id,
        region,
        units
    FROM metrics
) src
PIVOT (
    MAX(units) AS value_sum
    FOR region IN ('South' AS south, 'Central' AS central)
);
