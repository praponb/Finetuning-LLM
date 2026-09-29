WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC, id) AS row_num
    FROM suppliers
    QUALIFY row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(rating, 2) OVER w_part AS prev_rating,
        MAX(rating) OVER w_recent AS city_max_rating
    FROM ranked
    WINDOW w_part AS (ORDER BY rating, id), w_recent AS (PARTITION BY city, country)
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        top1.stock_qty
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched_suppliers AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        AVG(stock_qty) OVER w_ordered AS stock_qty_avg_6,
        DENSE_RANK() OVER w_recent AS stock_qty_dense_rank,
        LAST_VALUE(stock_qty) IGNORE NULLS OVER (PARTITION BY city ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty,
        LAG(stock_qty, 2) OVER (PARTITION BY city ORDER BY stock_qty, id) AS prev_stock_qty,
        SUM(stock_qty) OVER (PARTITION BY country ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty
    FROM with_latest
    WINDOW w_ordered AS (PARTITION BY country ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY city ORDER BY stock_qty DESC)
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.stock_qty_avg_6,
        s.prev_stock_qty,
        latest.cost,
        latest.launch_date
    FROM enriched_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.cost, c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', launch_date) AS activity_month,
        CASE WHEN stock_qty_avg_6 >= 411 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_avg_6_tier,
        COUNT(*) AS event_count,
        APPROX_PERCENTILE(cost, 0.95) AS cost_p95,
        ROUND(AVG(cost), 2) AS avg_cost,
        ROUND(AVG(stock_qty_avg_6), 2) AS avg_stock_qty_avg_6,
        COUNT(*) AS num_rows,
        COUNT(*) AS event_count_2
    FROM with_last
    GROUP BY DATE_TRUNC('MONTH', launch_date), CASE WHEN stock_qty_avg_6 >= 411 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    activity_month,
    stock_qty_avg_6_tier,
    event_count,
    cost_p95,
    avg_cost,
    avg_stock_qty_avg_6,
    num_rows
FROM stats
ORDER BY avg_stock_qty_avg_6 DESC NULLS LAST;
