WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        CAST(quantity AS STRING) AS quantity_str
    FROM order_items
    WHERE product_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_unit_price,
        AVG(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_unit_price,
        COUNT(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_unit_price
    FROM filtered
    WHERE quantity <= 5
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity NULLS LAST, id) = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last3_avg_unit_price DESC, id) AS rn
        FROM ranked
    ) x
    WHERE rn = 1
)
SELECT
    id,
    quantity,
    last3_sum_unit_price,
    last3_avg_unit_price,
    last3_count_unit_price,
    rn
FROM latest
ORDER BY last3_sum_unit_price DESC;
