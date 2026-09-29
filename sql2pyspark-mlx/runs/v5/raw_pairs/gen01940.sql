WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        quantity + unit_price AS line_total
    FROM order_items
    WHERE unit_price >= 210
),
with_dims AS (
    -- Attach orders attributes
    SELECT e.*, p.discount
    FROM valid e
    INNER JOIN orders p
        ON e.order_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        unit_price,
        LAG(discount, 1) OVER (PARTITION BY order_id ORDER BY unit_price ASC, id ASC) AS prev_discount,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY quantity DESC, id ASC) AS quantity_bucket,
        AVG(quantity) OVER w_ordered AS quantity_avg_5
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY quantity ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(quantity, unit_price) AS greatest_quantity_unit_price,
        CASE WHEN quantity_bucket >= 9 THEN 'High' WHEN quantity_bucket >= 2 THEN 'Medium' ELSE 'Low' END AS quantity_bucket_level
    FROM scored
    WHERE quantity_avg_5 <= 4
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        unit_price,
        prev_discount,
        quantity_bucket,
        quantity_avg_5,
        greatest_quantity_unit_price,
        quantity_bucket_level,
        quantity_bucket / NULLIF(prev_discount, 0) AS quantity_bucket_per_prev_discount
    FROM cleaned
    WHERE quantity_bucket = 5
),
scored_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(greatest_quantity_unit_price) IGNORE NULLS OVER w_running AS first_known_greatest_quantity_unit_price,
        SUM(unit_price) OVER (ORDER BY quantity_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price
    FROM base
    WHERE quantity_bucket <= 5
    WINDOW w_running AS (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    metric_name,
    metric_value
FROM (
    SELECT
        id,
        unit_price,
        first_known_greatest_quantity_unit_price,
        greatest_quantity_unit_price
    FROM scored_order_items
) src
UNPIVOT (
    metric_value FOR metric_name IN (unit_price, first_known_greatest_quantity_unit_price, greatest_quantity_unit_price)
);
