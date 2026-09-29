WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(revenue, 2) AS revenue_rounded,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE sale_date >= DATE '2023-04-18'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, store_id
    FROM cleaned
    EXCEPT
    SELECT product_id, store_id
    FROM store_sales
    WHERE store_id IN (3, 4, 6)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.store_id,
        a.product_id,
        a.sale_date,
        a.revenue_rounded,
        a.sale_date_month_end,
        s.city
    FROM verified a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.sqft,
        b.city AS store_city,
        b.store_name
    FROM enriched e
    LEFT JOIN stores b
        ON e.store_id = b.id
)
-- Join a dimension and aggregate
SELECT
    dim.category,
    s.store_city,
    COUNT(*) AS num_rows,
    MIN(s.revenue_rounded) AS min_revenue_rounded,
    MAX(s.sqft) AS max_sqft,
    GROUPING_ID() AS grouping_level
FROM with_dims s
LEFT JOIN products dim ON s.product_id = dim.id
GROUP BY ROLLUP (dim.category, s.store_city)
ORDER BY store_city DESC;
