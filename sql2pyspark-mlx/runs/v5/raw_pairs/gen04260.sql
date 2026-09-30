WITH kept_keys AS (
    -- Set difference on keys
    SELECT order_id, id
    FROM order_items
    EXCEPT ALL
    SELECT order_id, id
    FROM order_items
    WHERE quantity <> 5
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(unit_price / quantity, 2) AS line_total
    FROM verified
    WHERE order_id IN (1, 35)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share,
        PERCENT_RANK() OVER (PARTITION BY product_id, order_id ORDER BY line_total DESC NULLS FIRST) AS line_total_percent_rank,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity ASC NULLS FIRST) AS quantity_dense_rank
    FROM cleaned
    WHERE order_id IN (19, 40)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        line_total,
        line_total_percent_rank,
        DENSE_RANK() OVER (ORDER BY unit_price_share DESC NULLS FIRST) AS unit_price_share_dense_rank
    FROM metrics
)
SELECT
    id,
    order_id,
    quantity,
    unit_price,
    line_total_percent_rank,
    unit_price_share_dense_rank
FROM enriched
ORDER BY unit_price ASC NULLS LAST
