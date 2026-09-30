WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(unit_price, quantity) greatest_unit_price_quantity
    FROM order_items
    WHERE quantity > 7
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        FIRST_VALUE(unit_price, TRUE) OVER w_ordered first_known_unit_price,
        CUME_DIST() OVER w_recent greatest_unit_price_quantity_cume_dist,
        SUM(greatest_unit_price_quantity) OVER w_ordered cumulative_greatest_unit_price_quantity
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY product_id ORDER BY greatest_unit_price_quantity DESC NULLS LAST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(first_known_unit_price, 0) first_known_unit_price_filled
    FROM metrics
    WHERE product_id IS NOT NULL
),
joined AS (
    -- Attach orders attributes
    SELECT src.*, b.channel
    FROM valid src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
valid_order_items AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        first_known_unit_price,
        cumulative_greatest_unit_price_quantity,
        first_known_unit_price_filled - greatest_unit_price_quantity_cume_dist first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist
    FROM joined
    WHERE product_id IN (6, 15, 26, 36)
),
filtered_order_items AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        first_known_unit_price,
        cumulative_greatest_unit_price_quantity,
        first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist,
        unit_price * first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist unit_price_times_first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist,
        unit_price + first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist unit_price_plus_first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist
    FROM valid_order_items
    WHERE product_id IS NOT NULL
)
SELECT
    id,
    order_id,
    first_known_unit_price,
    cumulative_greatest_unit_price_quantity,
    first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist,
    unit_price_plus_first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist
FROM filtered_order_items;
