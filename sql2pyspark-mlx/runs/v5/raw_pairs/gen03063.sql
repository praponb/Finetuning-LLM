WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        brand,
        cost,
        stock_qty,
        launch_date,
        ROUND(cost / SUM(cost) OVER (PARTITION BY category), 4) AS cost_share,
        LAG(cost, 1) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS prev_cost,
        NTILE(5) OVER (PARTITION BY supplier_id ORDER BY cost, id) AS cost_bucket
    FROM products
    WHERE supplier_id IN (2, 30, 31, 40)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost_bucket) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_bucket,
        LAG(stock_qty, 1, 0) OVER (ORDER BY cost_bucket, id) AS prev_stock_qty,
        FIRST_VALUE(cost_bucket, TRUE) OVER w_recent AS first_known_cost_bucket
    FROM scored
    WINDOW w_recent AS (ORDER BY cost_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, brand
    FROM with_windows
    INTERSECT ALL
    SELECT id, brand
    FROM products
    WHERE category IN ('Grocery', 'Electronics', 'Books')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.brand = k.brand
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date < s.launch_date
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN first_known_cost_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_cost_bucket_tier,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(cost, 0.9) AS cost_p90,
    COUNT(*) AS event_count,
    AVG(prev_cost) FILTER (WHERE brand = 'Initech') AS initech_avg_prev_cost
FROM eligible
GROUP BY CASE WHEN first_known_cost_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING initech_avg_prev_cost < 1000000
ORDER BY event_count DESC;
