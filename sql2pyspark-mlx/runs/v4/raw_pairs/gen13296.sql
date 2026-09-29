WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(units, revenue) OVER (PARTITION BY product_id, store_id) AS corr_units_revenue,
        CORR(revenue, units) OVER (PARTITION BY product_id) AS corr_revenue_units,
        LEAD(revenue, 2) OVER (PARTITION BY store_id ORDER BY units, id) AS next_revenue
    FROM store_sales
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        p.sqft,
        p.region
    FROM enriched a
    INNER JOIN stores p
        ON a.store_id = p.id
),
with_dims_store_sales AS (
    -- Attach products attributes
    SELECT
        src.*,
        b.price,
        b.category
    FROM with_dims src
    LEFT JOIN products b
        ON src.product_id = b.id
),
enriched_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(next_revenue) IGNORE NULLS OVER (PARTITION BY product_id, store_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_revenue,
        AVG(corr_units_revenue) OVER (ORDER BY sale_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS corr_units_revenue_avg_5_prior,
        CORR(revenue, units) OVER (PARTITION BY store_id) AS corr_revenue_units_2,
        LEAD(corr_revenue_units, 1) OVER (PARTITION BY region ORDER BY corr_revenue_units, id) AS next_corr_revenue_units,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id) AS event_seq
    FROM with_dims_store_sales
),
with_dims_store_sales_2 AS (
    -- Attach stores attributes
    SELECT e.*, s.city
    FROM enriched_store_sales e
    LEFT JOIN stores s
        ON e.store_id = s.id
)
-- Grouped metrics
SELECT
    CASE WHEN sqft >= 16953 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    CASE WHEN next_corr_revenue_units >= 0.45 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_corr_revenue_units_tier,
    CASE WHEN last_known_next_revenue >= 4000 THEN 'HIGH' ELSE 'LOW' END AS last_known_next_revenue_tier,
    COUNT(*) FILTER (WHERE region = 'Central') AS central_rows,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    SIZE(COLLECT_SET(region)) AS num_regions
FROM with_dims_store_sales_2
GROUP BY sqft_tier, next_corr_revenue_units_tier, last_known_next_revenue_tier
