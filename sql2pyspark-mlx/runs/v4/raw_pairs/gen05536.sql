WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(unit_price, 0) unit_price_filled
    FROM order_items
    WHERE unit_price <= 170
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT product_id, order_id
    FROM valid
    INTERSECT
    SELECT product_id, order_id
    FROM order_items
    WHERE product_id = 6
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(quantity, unit_price_filled) least_quantity_unit_price_filled
    FROM verified
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_filled,
        least_quantity_unit_price_filled,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY least_quantity_unit_price_filled) least_quantity_unit_price_filled_dense_rank,
        CUME_DIST() OVER (ORDER BY unit_price DESC NULLS LAST) unit_price_cume_dist
    FROM base
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.unit_price,
    s.unit_price_filled,
    s.least_quantity_unit_price_filled,
    s.least_quantity_unit_price_filled_dense_rank,
    s.unit_price_cume_dist,
    (SELECT MAX(t2.quantity) FROM order_items t2 WHERE t2.product_id = s.product_id) AS max_order_item_quantity
FROM enriched s
