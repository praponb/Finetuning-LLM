WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY launch_date DESC NULLS LAST, id ASC) = 1
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        b.supplier_name,
        b.city,
        b.rating
    FROM latest e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_name,
        city,
        MIN(cost) OVER (PARTITION BY city, category ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cost,
        MAX(cost) OVER (PARTITION BY city, category ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_cost,
        AVG(cost) OVER (PARTITION BY city, category ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost
    FROM joined
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        running_max_cost,
        running_avg_cost,
        SUM(price) OVER w_ordered AS cumulative_price,
        LAG(price, 1) OVER w_part AS prev_price,
        ROW_NUMBER() OVER w_ordered_2 AS event_seq,
        MAX(price) OVER (PARTITION BY category) AS category_max_price,
        CORR(price, stock_qty) OVER (PARTITION BY city) AS corr_price_stock_qty
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY category ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY city ORDER BY running_max_cost ASC, id ASC), w_ordered_2 AS (PARTITION BY brand, city ORDER BY running_avg_cost DESC, id ASC)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    CASE WHEN running_avg_cost >= 810 THEN 'LARGE' ELSE 'SMALL' END AS running_avg_cost_tier,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    SIZE(COLLECT_SET(category)) AS num_categories,
    SUM(CASE WHEN brand = 'Initech' THEN price ELSE 0 END) AS initech_price,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN brand = 'Stark' THEN 1 END), 0), 2) AS stock_qty_per_stark
FROM metrics
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', launch_date), CASE WHEN running_avg_cost >= 810 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('WEEK', launch_date)))
ORDER BY stock_qty_per_stark DESC
