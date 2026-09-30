WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        unit_price,
        CEIL(quantity) AS quantity_whole
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        unit_price,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY unit_price ASC, id ASC) AS unit_price_bucket,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        LAST_VALUE(unit_price, TRUE) OVER (PARTITION BY product_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        FIRST_VALUE(quantity_whole) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_whole,
        LAST_VALUE(quantity_whole, TRUE) OVER (PARTITION BY product_id ORDER BY quantity_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_whole
    FROM valid
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN last_known_unit_price >= 340 THEN 'High' WHEN last_known_unit_price >= 170 THEN 'Medium' ELSE 'Low' END AS last_known_unit_price_band,
        last_known_quantity_whole * unit_price AS last_known_quantity_whole_times_unit_price
    FROM with_windows
    WHERE first_known_quantity_whole >= 79
),
base AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(last_known_quantity_whole) AS last_known_quantity_whole_whole
    FROM cleaned
    WHERE unit_price_bucket != 6
),
with_windows_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY first_known_unit_price ASC NULLS LAST) AS first_known_unit_price_cume_dist,
        DENSE_RANK() OVER (ORDER BY last_known_quantity_whole_times_unit_price DESC) AS last_known_quantity_whole_times_unit_price_dense_rank
    FROM base
)
SELECT
    id,
    unit_price_bucket,
    last_known_unit_price,
    last_known_quantity_whole,
    last_known_unit_price_band,
    last_known_quantity_whole_whole,
    last_known_quantity_whole_times_unit_price_dense_rank
FROM with_windows_order_items
ORDER BY last_known_quantity_whole DESC
