WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE last_restock > TO_DATE('2023-05-08')
      AND quantity > 127
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        quantity,
        last_restock
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity,
        MIN(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM latest
),
cleaned_inventory AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DAY(last_restock) AS last_restock_day,
        FLOOR(running_min_quantity) AS running_min_quantity_whole
    FROM metrics
    WHERE last_restock >= DATE '2023-04-07'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        store_id,
        CASE WHEN cumulative_quantity >= 1055 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_quantity_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
        COUNT(DISTINCT store_id) AS distinct_store_ids,
        GROUPING(store_id) AS grouping_id
    FROM cleaned_inventory
    GROUP BY ROLLUP (store_id, CASE WHEN cumulative_quantity >= 1055 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT
    s.store_id,
    s.cumulative_quantity_tier,
    s.distinct_ids,
    s.quantity_quartiles,
    s.distinct_store_ids,
    (SELECT MIN(o2.amount) FROM orders o2 WHERE o2.store_id = s.store_id) AS min_order_amount
FROM summary s
ORDER BY min_order_amount DESC NULLS FIRST, distinct_store_ids
