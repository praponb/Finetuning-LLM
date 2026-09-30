WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price - quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price >= 270
)
-- Window metrics over several different partitions
SELECT
    *,
    NTILE(10) OVER w_grp AS unit_price_bucket,
    LEAD(unit_price, 1, 0) OVER (PARTITION BY product_id ORDER BY unit_price ASC, id ASC) AS next_unit_price,
    NTILE(10) OVER (PARTITION BY order_id ORDER BY unit_price ASC, id ASC) AS unit_price_bucket_2,
    FIRST_VALUE(unit_price, TRUE) OVER w_ordered AS first_known_unit_price,
    NTILE(4) OVER w_grp AS unit_price_bucket_3
FROM cleaned
WINDOW w_grp AS (ORDER BY unit_price DESC, id ASC), w_ordered AS (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
