WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, WEEKOFYEAR(sale_date) AS sale_date_weekofyear
    FROM store_sales
    WHERE units >= 37
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(units, 1, 0) OVER w_hist AS next_units,
        LAST_VALUE(units, TRUE) OVER (ORDER BY sale_date_weekofyear, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units,
        FIRST_VALUE(revenue) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY sale_date_weekofyear, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue
    FROM base
    WINDOW w_hist AS (PARTITION BY store_id, product_id ORDER BY sale_date_weekofyear, id)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        sale_date_weekofyear,
        first_known_revenue,
        ROUND(last_known_units, 0) AS last_known_units_rounded
    FROM enriched
    WHERE sale_date > DATE '2023-03-07'
),
with_dims AS (
    -- Attach products attributes
    SELECT s.*, d.product_name
    FROM valid s
    INNER JOIN products d
        ON s.product_id = d.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(last_known_units_rounded, 0.9) AS last_known_units_rounded_p90
FROM with_dims
GROUP BY DATE_TRUNC('WEEK', sale_date)
HAVING APPROX_PERCENTILE(last_known_units_rounded, 0.9) > 0
ORDER BY distinct_ids;
