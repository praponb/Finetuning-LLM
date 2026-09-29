WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        stock_qty,
        launch_date,
        supplier_id,
        DATE_FORMAT(launch_date, 'yyyy-MM-dd') AS launch_date_label
    FROM products
    WHERE brand IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC NULLS FIRST, id) AS rank_in_group
        FROM valid
    ) t
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        b.city,
        b.supplier_name,
        b.rating
    FROM deduped e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(stock_qty) OVER (PARTITION BY category, city ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_stock_qty,
        COUNT(stock_qty) OVER (PARTITION BY category, city ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_stock_qty,
        AVG(stock_qty) OVER (PARTITION BY category, city ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_stock_qty,
        MAX(stock_qty) OVER (PARTITION BY category, city ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_stock_qty
    FROM enriched
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.sale_date <= '2024-02-01'
        )
),
with_windows_products AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS seq_no,
        NTILE(3) OVER w_recent AS running_avg_stock_qty_bucket,
        CUME_DIST() OVER (PARTITION BY category, city ORDER BY running_avg_stock_qty DESC) AS running_avg_stock_qty_cume_dist
    FROM kept
    WHERE city NOT IN ('Chiang Mai', 'Singapore', 'Berlin')
    WINDOW w_grp AS (PARTITION BY supplier_id ORDER BY launch_date, id), w_recent AS (PARTITION BY city, category ORDER BY running_avg_stock_qty, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(stock_qty) OVER (PARTITION BY category ORDER BY rating, id) AS next_stock_qty,
        LAG(stock_qty) OVER (PARTITION BY category ORDER BY rating, id) AS prev_stock_qty,
        LEAD(seq_no) OVER (PARTITION BY category ORDER BY rating, id) AS next_seq_no,
        LAG(seq_no) OVER (PARTITION BY category ORDER BY rating, id) AS prev_seq_no,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY rating, id) AS seq_no_2,
        LAST_VALUE(running_min_stock_qty) IGNORE NULLS OVER (ORDER BY running_count_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_min_stock_qty
    FROM with_windows_products
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_stock_qty >= 323 THEN 'HIGH' ELSE 'LOW' END AS next_stock_qty_tier,
    COUNT_IF(category = 'Books') AS books_events,
    SUM(CASE WHEN city = 'Bangkok' THEN running_min_stock_qty ELSE 0 END) AS bangkok_running_min_stock_qty
FROM metrics
GROUP BY CASE WHEN next_stock_qty >= 323 THEN 'HIGH' ELSE 'LOW' END
ORDER BY next_stock_qty_tier DESC
