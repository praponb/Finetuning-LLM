WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        DATE_ADD(last_restock, 15) AS last_restock_plus_15d,
        CAST(quantity AS DOUBLE) AS quantity_num
    FROM inventory
    WHERE last_restock > DATE '2023-06-10'
),
enriched AS (
    -- Attach products attributes
    SELECT src.*, b.cost
    FROM base src
    INNER JOIN products b
        ON src.product_id = b.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        AVG(quantity) OVER w_ordered AS quantity_avg_2,
        SUM(cost) OVER (ORDER BY quantity_num ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        SUM(quantity) OVER w_running AS running_quantity
    FROM latest
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_running AS (ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    APPROX_PERCENTILE(running_cost, 0.95) AS running_cost_p95,
    MAX(quantity_num) AS max_quantity_num
FROM with_windows
GROUP BY product_id
ORDER BY running_cost_p95 DESC;
