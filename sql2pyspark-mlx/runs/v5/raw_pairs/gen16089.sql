WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(revenue) OVER w_running AS last7_sum_revenue,
        COUNT(revenue) OVER w_running AS last7_count_revenue,
        MAX(revenue) OVER w_running AS last7_max_revenue,
        AVG(revenue) OVER w_running AS last7_avg_revenue
    FROM store_sales
    WINDOW w_running AS (PARTITION BY store_id, product_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT ALL
    SELECT id
    FROM store_sales
    WHERE units <= 39
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(last7_count_revenue / last7_avg_revenue, 2) AS last7_count_revenue_per_last7_avg_revenue
    FROM reconciled
    WHERE sale_date > DATE '2023-02-25'
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.stock_qty,
        p.launch_date,
        p.price
    FROM filtered s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE p.category IN ('Fashion', 'Books', 'Grocery')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        last7_sum_revenue,
        last7_max_revenue,
        last7_avg_revenue,
        CUME_DIST() OVER (ORDER BY last7_avg_revenue DESC NULLS FIRST) AS last7_avg_revenue_cume_dist
    FROM joined
)
-- Join a dimension and aggregate
SELECT
    d.brand,
    d.category,
    COUNT(*) AS cnt,
    MAX(e.last7_sum_revenue) AS max_last7_sum_revenue
FROM enriched e
INNER JOIN products d ON e.product_id = d.id
GROUP BY d.brand, d.category
ORDER BY category
