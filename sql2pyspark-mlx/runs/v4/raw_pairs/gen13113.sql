WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity - unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT product_id, order_id
    FROM valid
    EXCEPT DISTINCT
    SELECT product_id, order_id
    FROM order_items
    WHERE product_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(quantity, unit_price) OVER (PARTITION BY product_id) AS corr_quantity_unit_price,
        RANK() OVER (ORDER BY quantity DESC NULLS LAST) AS quantity_rank
    FROM verified
)
SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    corr_quantity_unit_price,
    quantity_rank
FROM scored
ORDER BY quantity_rank DESC, corr_quantity_unit_price;
