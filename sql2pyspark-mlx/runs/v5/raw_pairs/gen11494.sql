WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(units, 0) AS units_filled
    FROM store_sales
    WHERE sale_date >= DATE '2023-04-07'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC NULLS LAST, id ASC) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(revenue, units) OVER w_ordered AS corr_revenue_units,
        SUM(revenue) OVER (PARTITION BY store_id ORDER BY sale_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
        NTILE(3) OVER (ORDER BY units ASC, id ASC) AS units_bucket
    FROM latest
    WHERE revenue BETWEEN 2500 AND 3800
    WINDOW w_ordered AS (PARTITION BY store_id)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, units_bucket + corr_revenue_units AS units_bucket_plus_corr_revenue_units
    FROM metrics
    WHERE product_id IN (3, 8, 9, 10)
),
latest_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        product_id,
        revenue,
        rn,
        running_revenue
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units_bucket DESC, id ASC) AS latest_rank
        FROM valid
    ) x
    WHERE latest_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN running_revenue >= 19872 THEN 'LARGE' ELSE 'SMALL' END AS running_revenue_tier,
    store_id,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    APPROX_PERCENTILE(running_revenue, 0.95) AS running_revenue_p95,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN running_revenue >= 19872 THEN 'LARGE' ELSE 'SMALL' END ORDER BY APPROX_PERCENTILE(running_revenue, 0.95) DESC) AS running_revenue_p95_percent_rank
FROM latest_store_sales
GROUP BY running_revenue_tier, store_id;
