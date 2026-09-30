WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY supplier_id, category ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost,
        COUNT(cost) OVER (PARTITION BY supplier_id, category ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost,
        MIN(cost) OVER (PARTITION BY supplier_id, category ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cost
    FROM latest
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        p.city,
        p.country,
        p.rating
    FROM metrics s
    JOIN suppliers p
        ON s.supplier_id = p.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY running_avg_cost DESC NULLS LAST, id) AS row_num
    FROM joined
    QUALIFY row_num = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND MONTH(x.sale_date) IN (6, 7, 9)
        )
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
        supplier_id,
        AVG(stock_qty) AS avg_stock_qty,
        SORT_ARRAY(COLLECT_SET(category)) AS category_list,
        PERCENTILE_APPROX(cost, 0.95) AS cost_p95,
        COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count,
        SUM(CASE WHEN brand = 'Umbrella' THEN running_count_cost ELSE 0 END) AS umbrella_running_count_cost,
        SUM(CASE WHEN category = 'Electronics' THEN price ELSE 0 END) AS electronics_price
    FROM kept
    GROUP BY DATE_TRUNC('QUARTER', launch_date), supplier_id
    HAVING SUM(CASE WHEN brand = 'Umbrella' THEN running_count_cost ELSE 0 END) > 0
)
SELECT
    s.quarter_start,
    s.supplier_id,
    s.category_list,
    s.acme_count,
    (SELECT AVG(t2.cost) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS avg_product_cost
FROM summary s
