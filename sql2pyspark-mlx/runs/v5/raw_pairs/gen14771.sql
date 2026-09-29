WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_name,
        cost,
        stock_qty,
        launch_date,
        tags
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost ASC NULLS FIRST, id ASC) AS rank_in_group
        FROM products
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY cost DESC NULLS FIRST) AS cost_percent_rank,
        LEAD(cost, 2) OVER (ORDER BY stock_qty ASC, id ASC) AS next_cost,
        NTILE(10) OVER (ORDER BY cost DESC, id ASC) AS cost_bucket
    FROM deduped
    WHERE launch_date >= '2024-11-01'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        cost,
        tags,
        AVG(stock_qty) OVER (ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_stock_qty,
        SUM(stock_qty) OVER (ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_stock_qty,
        COUNT(stock_qty) OVER (ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_stock_qty,
        MIN(stock_qty) OVER (ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_stock_qty
    FROM with_windows
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.store_sale_sale_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics_products AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        running_avg_stock_qty,
        running_sum_stock_qty,
        running_count_stock_qty,
        SUM(running_avg_stock_qty) OVER w_part AS running_sum_running_avg_stock_qty,
        COUNT(running_avg_stock_qty) OVER w_part AS running_count_running_avg_stock_qty,
        MAX(running_avg_stock_qty) OVER w_part AS running_max_running_avg_stock_qty
    FROM with_latest
    WINDOW w_part AS (ORDER BY running_min_stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN running_count_stock_qty >= 13 THEN 'LARGE' ELSE 'SMALL' END AS running_count_stock_qty_tier,
        AVG(running_count_stock_qty) AS avg_running_count_stock_qty,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids_2,
        ROUND(AVG(running_sum_running_avg_stock_qty), 2) AS avg_running_sum_running_avg_stock_qty,
        APPROX_PERCENTILE(running_avg_stock_qty, 0.9) AS running_avg_stock_qty_p90
    FROM metrics_products
    GROUP BY 1
)
SELECT
    running_count_stock_qty_tier,
    avg_running_count_stock_qty,
    distinct_ids,
    row_count,
    avg_running_sum_running_avg_stock_qty
FROM summary
ORDER BY avg_running_count_stock_qty DESC;
