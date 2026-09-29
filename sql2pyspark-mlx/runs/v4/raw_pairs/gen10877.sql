WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p.region,
        p.opened_date,
        p.sqft
    FROM store_sales s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p.opened_date AS store_opened_date,
        p.region AS store_region,
        p.store_name
    FROM with_dims s
    INNER JOIN stores p
        ON s.store_id = p.id
),
with_dims_store_sales AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.launch_date,
        p.cost,
        p.brand
    FROM enriched a
    INNER JOIN products p
        ON a.product_id = p.id
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
        SUM(revenue) AS total_revenue,
        ROUND(SUM(units) / NULLIF(COUNT(CASE WHEN brand = 'Initech' THEN 1 END), 0), 2) AS units_per_initech,
        COUNT(*) FILTER (WHERE brand = 'Umbrella') AS umbrella_rows,
        APPROX_PERCENTILE(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
        SIZE(COLLECT_SET(brand)) AS num_brands
    FROM with_dims_store_sales
    GROUP BY DATE_TRUNC('QUARTER', opened_date)
)
SELECT
    quarter_start,
    total_revenue,
    units_per_initech,
    umbrella_rows,
    revenue_quartiles,
    num_brands
FROM stats;
