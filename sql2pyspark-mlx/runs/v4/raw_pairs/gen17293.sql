WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        unit_price,
        ROUND(quantity, 2) quantity_rounded,
        CAST(unit_price AS INT) unit_price_int
    FROM order_items
    WHERE order_id = 28
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        unit_price,
        unit_price_int,
        CASE WHEN unit_price >= 350 THEN 'High' WHEN unit_price >= 190 THEN 'Medium' ELSE 'Low' END unit_price_tier
    FROM filtered
    WHERE product_id IS NOT NULL
      AND unit_price BETWEEN 310 AND 370
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(unit_price, TRUE) OVER w_recent first_known_unit_price,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER w_recent first_known_unit_price_2,
        LEAD(unit_price_int, 1, 0) OVER w_running next_unit_price_int,
        RANK() OVER (ORDER BY unit_price_int DESC NULLS LAST) unit_price_int_rank,
        NTILE(10) OVER w_grp unit_price_int_bucket
    FROM valid
    WINDOW w_recent AS (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY unit_price_int ASC, id ASC), w_grp AS (ORDER BY unit_price_int DESC, id ASC)
)
SELECT
    id,
    unit_price,
    unit_price_int,
    unit_price_tier,
    next_unit_price_int,
    unit_price_int_rank
FROM with_windows
ORDER BY id DESC NULLS FIRST;
