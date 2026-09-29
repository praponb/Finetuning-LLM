WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE unit_price >= 210
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        unit_price,
        SUM(quantity) OVER (PARTITION BY order_id ORDER BY unit_price_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        SUM(unit_price) OVER (ORDER BY unit_price_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        AVG(quantity) OVER (ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id) AS corr_quantity_unit_price,
        COUNT(unit_price) OVER (PARTITION BY order_id) AS order_id_count_unit_price
    FROM valid
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        quantity - cumulative_quantity AS quantity_minus_cumulative_quantity,
        cumulative_quantity - quantity AS cumulative_quantity_minus_quantity
    FROM scored
    WHERE order_id_count_unit_price < 10
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (ORDER BY cumulative_quantity_minus_quantity, id) AS cumulative_quantity_minus_quantity_bucket,
        LAG(unit_price, 2) OVER (ORDER BY cumulative_quantity, id) AS prev_unit_price
    FROM base
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    measure,
    value
FROM (
    SELECT
        id,
        unit_price,
        corr_quantity_unit_price,
        quantity_minus_cumulative_quantity
    FROM with_windows
) src
UNPIVOT (
    value FOR measure IN (unit_price AS UNIT_PRICE, corr_quantity_unit_price AS CORR_QUANTITY_UNIT_PRICE, quantity_minus_cumulative_quantity)
)
