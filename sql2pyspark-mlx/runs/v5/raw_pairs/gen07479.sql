WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        sale_date,
        revenue,
        ROUND(revenue * 1.07, 2) AS revenue_scaled
    FROM store_sales
    WHERE units <= 25
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(revenue, revenue_scaled) AS least_revenue_revenue_scaled,
        revenue_scaled + revenue AS revenue_scaled_plus_revenue
    FROM valid
    WHERE revenue >= 3900
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        sale_date,
        revenue,
        revenue_scaled_plus_revenue,
        CASE WHEN revenue >= 4000 THEN 'High' WHEN revenue >= 700 THEN 'Medium' ELSE 'Low' END AS revenue_band
    FROM filtered
    WHERE revenue_scaled <= 49
),
filtered_store_sales AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        revenue / NULLIF(revenue_scaled_plus_revenue, 0) AS revenue_per_revenue_scaled_plus_revenue,
        ADD_MONTHS(sale_date, 6) AS sale_date_plus_months
    FROM base
    WHERE sale_date >= DATE '2023-04-11'
)
SELECT
    id,
    sale_date,
    revenue,
    revenue_scaled_plus_revenue,
    revenue_band,
    revenue_per_revenue_scaled_plus_revenue,
    sale_date_plus_months
FROM filtered_store_sales
ORDER BY revenue NULLS LAST;
