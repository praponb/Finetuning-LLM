WITH with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY quantity DESC, id) AS row_seq,
        LEAD(unit_price, 2) OVER (ORDER BY quantity, id) AS next_unit_price
    FROM order_items
),
joined AS (
    SELECT e.*, o.status
    FROM with_windows e
    LEFT JOIN orders o
        ON e.order_id = o.id
    WHERE o.status IN ('PENDING', 'SHIPPED', 'CANCELLED')
),
cleaned AS (
    SELECT
        *,
        ROUND(quantity / unit_price, 2) AS line_total,
        ROUND(row_seq * 100, 2) AS adjusted_row_seq,
        COALESCE(status, 'OTHER') AS resolved_status
    FROM joined
    WHERE product_id IS NOT NULL
      AND status IS DISTINCT FROM 'SHIPPED'
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS rank_in_group
        FROM cleaned
    ) ranked
    WHERE rank_in_group = 1
),
ranked AS (
    SELECT
        id,
        order_id,
        unit_price,
        next_unit_price,
        status,
        resolved_status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, status ORDER BY line_total, id) AS row_num
        FROM latest
    ) x
    WHERE row_num = 1
),
valid AS (
    SELECT
        id,
        order_id,
        unit_price,
        next_unit_price,
        status,
        resolved_status,
        FLOOR(next_unit_price) AS next_unit_price_whole
    FROM ranked
    WHERE status IS NOT NULL
),
base AS (
    SELECT
        id,
        order_id,
        next_unit_price,
        resolved_status,
        next_unit_price_whole,
        CASE WHEN next_unit_price_whole >= 71 THEN 'High' WHEN next_unit_price_whole >= 27 THEN 'Medium' ELSE 'Low' END AS next_unit_price_whole_band
    FROM valid
    WHERE status IS NOT NULL
)
SELECT
    id,
    order_id,
    next_unit_price_whole,
    MAX(next_unit_price) OVER w_recent AS running_max_next_unit_price,
    SUM(next_unit_price) OVER w_recent AS running_sum_next_unit_price,
    COUNT(next_unit_price) OVER w_recent AS running_count_next_unit_price,
    AVG(next_unit_price) OVER w_recent AS running_avg_next_unit_price,
    LAST_VALUE(next_unit_price) IGNORE NULLS OVER w_recent AS last_known_next_unit_price
FROM base
WINDOW w_recent AS (PARTITION BY order_id ORDER BY next_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
