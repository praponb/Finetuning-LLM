WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        MIN(unit_price) OVER w_grp AS running_min_unit_price,
        AVG(unit_price) OVER w_grp AS running_avg_unit_price,
        SUM(unit_price) OVER w_grp AS running_sum_unit_price,
        SUM(quantity) OVER w_grp AS cumulative_quantity
    FROM order_items
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach products attributes
    SELECT s.*, p.category
    FROM with_windows s
    INNER JOIN products p
        ON s.product_id = p.id
    WHERE p.product_name LIKE 'R%'
),
with_windows_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(running_min_unit_price) OVER (PARTITION BY order_id ORDER BY cumulative_quantity, id) AS next_running_min_unit_price,
        LAG(running_min_unit_price) OVER (PARTITION BY order_id ORDER BY cumulative_quantity, id) AS prev_running_min_unit_price,
        LAG(running_sum_unit_price) OVER (PARTITION BY order_id ORDER BY cumulative_quantity, id) AS prev_running_sum_unit_price,
        LEAD(running_sum_unit_price) OVER (PARTITION BY order_id ORDER BY cumulative_quantity, id) AS next_running_sum_unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cumulative_quantity, id) AS event_seq
    FROM with_dims
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        product_id,
        running_sum_unit_price,
        cumulative_quantity,
        next_running_min_unit_price,
        prev_running_min_unit_price,
        prev_running_sum_unit_price,
        next_running_sum_unit_price
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY running_sum_unit_price DESC NULLS LAST) AS latest_rank
        FROM with_windows_order_items
    ) x
    WHERE latest_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(running_sum_unit_price AS INT) AS running_sum_unit_price_int,
        ROUND(cumulative_quantity, 2) AS cumulative_quantity_rounded
    FROM ranked
    WHERE product_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT product_id, id
    FROM order_items
    WHERE quantity >= 4
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    id,
    running_sum_unit_price,
    prev_running_min_unit_price,
    next_running_sum_unit_price,
    cumulative_quantity_rounded
FROM kept_rows
ORDER BY id DESC NULLS LAST
