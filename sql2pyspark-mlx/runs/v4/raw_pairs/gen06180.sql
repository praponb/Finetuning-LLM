WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity
    FROM filtered
    WHERE product_id IS NOT NULL
      AND unit_price < 290
),
base_order_items AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        greatest_quantity_unit_price,
        greatest_unit_price_quantity,
        quantity + greatest_unit_price_quantity AS quantity_plus_greatest_unit_price_quantity
    FROM base
    WHERE quantity != 8
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(greatest_quantity_unit_price) OVER w_ordered AS greatest_quantity_unit_price_avg_3,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY greatest_unit_price_quantity, id) AS seq_no,
        SUM(greatest_unit_price_quantity) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_greatest_unit_price_quantity,
        LAST_VALUE(unit_price) IGNORE NULLS OVER w_hist AS last_known_unit_price,
        MAX(quantity) OVER w_recent AS order_id_max_quantity
    FROM base_order_items
    WINDOW w_ordered AS (ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY order_id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id, product_id ORDER BY running_greatest_unit_price_quantity DESC) AS rank_in_group
        FROM with_windows
    ) ranked
    WHERE rank_in_group = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        greatest_quantity_unit_price,
        greatest_unit_price_quantity,
        quantity_plus_greatest_unit_price_quantity,
        greatest_quantity_unit_price_avg_3,
        seq_no,
        running_greatest_unit_price_quantity,
        last_known_unit_price,
        order_id_max_quantity,
        rank_in_group,
        RANK() OVER (ORDER BY last_known_unit_price DESC) AS last_known_unit_price_rank,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY last_known_unit_price DESC, id) AS last_known_unit_price_bucket
    FROM ranked
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.unit_price,
    s.quantity_plus_greatest_unit_price_quantity,
    s.order_id_max_quantity,
    s.last_known_unit_price_bucket,
    (SELECT MAX(x.quantity) FROM order_items x WHERE x.order_id = s.order_id) AS max_order_item_quantity
FROM metrics s
ORDER BY unit_price
