WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2024-04-19'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY revenue_per_unit DESC) AS revenue_per_unit_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units, id) AS row_seq
    FROM cleaned
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        s.region,
        s.sqft,
        s.opened_date
    FROM enriched a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_per_unit,
        revenue_per_unit_cume_dist,
        row_seq,
        region,
        sqft,
        opened_date,
        AVG(units) OVER (ORDER BY revenue_per_unit, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS units_avg_3,
        SUM(revenue_per_unit) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue_per_unit,
        SUM(revenue_per_unit) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue_per_unit
    FROM with_dims
),
joined AS (
    -- Attach stores attributes
    SELECT
        a.*,
        d.city,
        d.opened_date AS store_opened_date
    FROM with_windows a
    INNER JOIN stores d
        ON a.store_id = d.id
),
with_windows_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(revenue_per_unit_cume_dist, TRUE) OVER (PARTITION BY city, store_id ORDER BY store_opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue_per_unit_cume_dist,
        SUM(units) OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units,
        SUM(sqft) OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        ROUND(units / SUM(units) OVER (PARTITION BY city, store_id), 4) AS units_share
    FROM joined
)
-- Grouped metrics
SELECT
    CASE WHEN revenue_per_unit_cume_dist >= 0.71 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_per_unit_cume_dist_tier,
    COUNT(*) FILTER (WHERE region = 'North') AS north_rows,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    ROUND(SUM(units_avg_3) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS units_avg_3_per_new_york,
    COUNT(DISTINCT city) AS distinct_cities
FROM with_windows_store_sales
GROUP BY CASE WHEN revenue_per_unit_cume_dist >= 0.71 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY units_avg_3_per_new_york DESC NULLS LAST
