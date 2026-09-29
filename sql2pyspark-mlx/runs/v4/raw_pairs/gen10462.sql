WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        CEIL(revenue) AS revenue_whole,
        LEAST(revenue, units) AS least_revenue_units
    FROM store_sales
    WHERE product_id IN (1, 7, 8, 10)
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.region,
        d.store_name,
        d.sqft
    FROM valid e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_whole,
        least_revenue_units,
        region,
        store_name,
        sqft,
        LEAD(revenue_whole, 1) OVER (PARTITION BY region ORDER BY least_revenue_units ASC, id ASC) AS next_revenue_whole,
        CUME_DIST() OVER (ORDER BY revenue_whole DESC NULLS FIRST) AS revenue_whole_cume_dist
    FROM joined
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(revenue_whole_cume_dist, TRUE) OVER (PARTITION BY product_id ORDER BY revenue_whole_cume_dist ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue_whole_cume_dist,
        LEAD(revenue_whole, 1) OVER (ORDER BY revenue ASC, id ASC) AS next_revenue_whole_2
    FROM scored
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    CASE WHEN units >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_tier,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    ROUND(AVG(last_known_revenue_whole_cume_dist), 2) AS avg_last_known_revenue_whole_cume_dist,
    COUNT_IF(region = 'North') AS north_events,
    COUNT(*) FILTER (WHERE region = 'Central') AS central_rows,
    ROUND(SUM(revenue_whole) / NULLIF(COUNT(CASE WHEN region = 'South' THEN 1 END), 0), 2) AS revenue_whole_per_south,
    SUM(revenue_whole_cume_dist) AS total_revenue_whole_cume_dist
FROM enriched
GROUP BY DATE_TRUNC('QUARTER', sale_date), CASE WHEN units >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY quarter_start DESC;
