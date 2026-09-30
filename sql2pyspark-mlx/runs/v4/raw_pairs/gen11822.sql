WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        unit_price - quantity AS line_total
    FROM order_items
    WHERE unit_price < 230
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(unit_price) OVER (PARTITION BY order_id ORDER BY line_total, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_avg_6_prior,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY line_total, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_avg_2
    FROM valid
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY line_total DESC, id) = 1
)
SELECT
    id,
    order_id,
    product_id,
    unit_price,
    unit_price_avg_2
FROM latest
ORDER BY unit_price DESC NULLS FIRST, order_id DESC;
