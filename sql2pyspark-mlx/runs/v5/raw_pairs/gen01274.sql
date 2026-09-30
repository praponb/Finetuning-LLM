WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        units,
        revenue,
        LAG(units, 1) OVER (PARTITION BY product_id, store_id ORDER BY units, id) prev_units,
        CORR(revenue, units) OVER (PARTITION BY store_id) corr_revenue_units
    FROM store_sales
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(corr_revenue_units / revenue, 2) corr_revenue_units_per_revenue
    FROM with_windows
    WHERE units <= 22
),
with_windows_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY prev_units DESC, id) row_seq,
        LAG(corr_revenue_units_per_revenue, 1, 0) OVER (ORDER BY prev_units, id) prev_corr_revenue_units_per_revenue
    FROM filtered
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        prev_corr_revenue_units_per_revenue + prev_units prev_corr_revenue_units_per_revenue_plus_prev_units,
        CAST(prev_corr_revenue_units_per_revenue AS INT) prev_corr_revenue_units_per_revenue_int
    FROM with_windows_store_sales
    WHERE row_seq >= 7
)
-- Grouped metrics
SELECT
    store_id,
    CASE WHEN corr_revenue_units >= 0.58 THEN 'HIGH' ELSE 'LOW' END AS corr_revenue_units_tier,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    AVG(revenue) AS avg_revenue,
    ROUND(AVG(units), 2) AS avg_units
FROM base
GROUP BY store_id, CASE WHEN corr_revenue_units >= 0.58 THEN 'HIGH' ELSE 'LOW' END
