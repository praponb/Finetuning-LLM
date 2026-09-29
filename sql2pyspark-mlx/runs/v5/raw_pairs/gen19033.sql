WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
    FROM store_sales
    WHERE revenue >= 2100
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(units) OVER w_recent AS running_max_units,
        AVG(units) OVER w_recent AS running_avg_units,
        MIN(units) OVER w_recent AS running_min_units
    FROM valid
    WHERE units <> 11
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY sale_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY running_avg_units DESC, id ASC) AS running_avg_units_bucket,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY units DESC NULLS LAST) AS units_cume_dist,
        AVG(units) OVER (PARTITION BY product_id) AS product_id_avg_units,
        SUM(revenue) OVER (PARTITION BY product_id) AS product_id_sum_revenue
    FROM with_windows
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN units_cume_dist >= 0.86 THEN 'High' WHEN units_cume_dist >= 0.32 THEN 'Medium' ELSE 'Low' END AS units_cume_dist_level,
        CASE WHEN units_cume_dist >= 0.9 THEN 'High' WHEN units_cume_dist >= 0.12 THEN 'Medium' ELSE 'Low' END AS units_cume_dist_level_2
    FROM with_windows_store_sales
    WHERE units_cume_dist BETWEEN 0.28 AND 0.44
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN running_avg_units_bucket >= 9 THEN 'High' WHEN running_avg_units_bucket >= 2 THEN 'Medium' ELSE 'Low' END AS running_avg_units_bucket_tier
    FROM cleaned
    WHERE product_id IN (3, 9)
),
valid_store_sales AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(units_cume_dist AS INT) AS units_cume_dist_int,
        ROUND(running_avg_units * 1.15, 2) AS adjusted_running_avg_units
    FROM base
    WHERE store_id = 3
)
-- Grouped metrics
SELECT
    CASE WHEN revenue >= 3800 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(running_avg_units_bucket, 0.5) AS running_avg_units_bucket_p50,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN revenue >= 3800 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) DESC) AS row_count_percent_rank
FROM valid_store_sales
GROUP BY CASE WHEN revenue >= 3800 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', sale_date)
HAVING row_count > 1;
