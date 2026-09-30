WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        CAST(unit_price AS INT) AS unit_price_int,
        quantity * unit_price AS line_total
    FROM order_items
    WHERE order_id IN (1, 11, 17)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        unit_price_int,
        CAST(line_total AS INT) AS line_total_int,
        unit_price + line_total AS unit_price_plus_line_total
    FROM valid
    WHERE product_id IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(unit_price, 1) AS unit_price_rounded
    FROM filtered
    WHERE product_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        line_total_int,
        unit_price_plus_line_total,
        SUM(unit_price_rounded) OVER (PARTITION BY product_id ORDER BY unit_price_plus_line_total ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_rounded,
        DENSE_RANK() OVER (PARTITION BY product_id, order_id ORDER BY unit_price_rounded DESC NULLS FIRST) AS unit_price_rounded_dense_rank
    FROM cleaned
    WHERE product_id IS NULL
)
-- Ranking and gap features
SELECT
    id,
    product_id,
    unit_price,
    line_total_int,
    running_unit_price_rounded,
    NTILE(3) OVER (PARTITION BY product_id ORDER BY unit_price DESC, id ASC) AS unit_price_bucket,
    LAG(unit_price_rounded_dense_rank, 2) OVER (ORDER BY running_unit_price_rounded ASC, id ASC) AS prev_unit_price_rounded_dense_rank
FROM scored
