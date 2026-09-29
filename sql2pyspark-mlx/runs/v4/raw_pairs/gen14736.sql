WITH base AS (
    -- Keep recent, valid rows
    SELECT *, GREATEST(units, revenue) AS greatest_units_revenue
    FROM store_sales
    WHERE revenue BETWEEN 1600 AND 3200
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.launch_date,
        p.price
    FROM base e
    LEFT JOIN products p
        ON e.product_id = p.id
),
with_dims AS (
    -- Attach products attributes
    SELECT src.*, p.category
    FROM enriched src
    LEFT JOIN products p
        ON src.product_id = p.id
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        greatest_units_revenue,
        launch_date,
        price,
        category,
        ROW_NUMBER() OVER (PARTITION BY product_id, category ORDER BY greatest_units_revenue DESC, id) AS event_seq,
        ROUND(price / SUM(price) OVER (PARTITION BY store_id), 4) AS price_share,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id, store_id), 4) AS revenue_share,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY greatest_units_revenue, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS revenue_avg_2
    FROM with_dims
),
enriched_store_sales_2 AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.launch_date AS product_launch_date,
        p.stock_qty,
        p.brand
    FROM enriched_store_sales a
    JOIN products p
        ON a.product_id = p.id
),
enriched_store_sales_3 AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.opened_date,
        d.sqft,
        d.region
    FROM enriched_store_sales_2 s
    LEFT JOIN stores d
        ON s.store_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    CASE WHEN greatest_units_revenue >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS greatest_units_revenue_tier,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    SUM(CASE WHEN brand = 'Stark' THEN price_share ELSE 0 END) AS stark_price_share,
    GROUPING_ID() AS grouping_id
FROM enriched_store_sales_3
GROUP BY GROUPING SETS ((product_id, CASE WHEN greatest_units_revenue >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END), (product_id), ())
ORDER BY product_id DESC
