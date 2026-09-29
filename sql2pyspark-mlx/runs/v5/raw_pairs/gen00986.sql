WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ROUND(revenue * 100, 2) AS revenue_adjusted
    FROM store_sales
    WHERE product_id IN (7, 8)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        sale_date,
        units,
        revenue,
        LEAD(revenue_adjusted, 1) OVER w_ordered AS next_revenue_adjusted,
        LAG(revenue_adjusted, 2) OVER (ORDER BY revenue, id) AS prev_revenue_adjusted,
        SUM(revenue_adjusted) OVER (PARTITION BY store_id) AS store_id_sum_revenue_adjusted
    FROM valid
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY revenue, id)
),
metrics_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        sale_date,
        units,
        revenue,
        next_revenue_adjusted,
        LAST_VALUE(store_id_sum_revenue_adjusted) IGNORE NULLS OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_store_id_sum_revenue_adjusted,
        NTILE(3) OVER (ORDER BY revenue, id) AS revenue_bucket,
        SUM(prev_revenue_adjusted) OVER (ORDER BY next_revenue_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_revenue_adjusted
    FROM metrics
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(revenue) OVER w_grp AS next_revenue,
        LEAD(revenue_bucket) OVER w_grp AS next_revenue_bucket,
        LAG(revenue_bucket) OVER w_grp AS prev_revenue_bucket,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM metrics_store_sales
    WHERE last_known_store_id_sum_revenue_adjusted <= 151
    WINDOW w_grp AS (ORDER BY next_revenue_adjusted, id)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM store_sales
    WHERE store_id = 1
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
),
valid_store_sales AS (
    -- Keep recent, valid rows
    SELECT *, CAST(seq_no AS DOUBLE) AS seq_no_str
    FROM reconciled
    WHERE sale_date >= DATE '2023-07-05'
),
valid_store_sales_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN seq_no_str >= 88 THEN 'High' WHEN seq_no_str >= 20 THEN 'Medium' ELSE 'Low' END AS seq_no_str_band,
        CASE WHEN last_known_store_id_sum_revenue_adjusted >= 326 THEN 'High' WHEN last_known_store_id_sum_revenue_adjusted >= 149 THEN 'Medium' ELSE 'Low' END AS last_known_store_id_sum_revenue_adjusted_level
    FROM valid_store_sales
    WHERE sale_date > TO_DATE('2023-08-19')
      AND next_revenue_bucket BETWEEN 3 AND 4
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN revenue >= 3500 THEN 'LARGE' ELSE 'SMALL' END AS revenue_tier,
        CASE WHEN units >= 37 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_tier,
        seq_no,
        MAX(cumulative_prev_revenue_adjusted) AS max_cumulative_prev_revenue_adjusted,
        MIN(revenue_bucket) AS min_revenue_bucket
    FROM valid_store_sales_2
    GROUP BY CASE WHEN revenue >= 3500 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN units >= 37 THEN 'PREMIUM' ELSE 'STANDARD' END, seq_no
    HAVING max_cumulative_prev_revenue_adjusted >= 0
)
SELECT
    revenue_tier,
    units_tier,
    seq_no,
    max_cumulative_prev_revenue_adjusted,
    min_revenue_bucket
FROM grouped
ORDER BY seq_no;
