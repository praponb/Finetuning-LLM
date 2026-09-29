WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        quantity,
        quantity - unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (4, 10, 21)
),
with_dims AS (
    -- Attach orders attributes
    SELECT src.*, o.amount
    FROM base src
    INNER JOIN orders o
        ON src.order_id = o.id
    WHERE o.status = 'CANCELLED'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        line_total,
        amount,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY line_total, id) AS line_total_bucket,
        SUM(quantity) OVER (ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
    FROM with_dims
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(amount) OVER w_grp AS next_amount,
        LAG(line_total) OVER w_grp AS prev_line_total,
        ROW_NUMBER() OVER w_grp AS event_seq,
        NTILE(5) OVER w_ordered AS line_total_bucket_2
    FROM enriched
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY line_total, id), w_ordered AS (PARTITION BY order_id ORDER BY line_total DESC, id)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, order_id
    FROM with_windows
    EXCEPT
    SELECT id, order_id
    FROM order_items
    WHERE product_id = 9
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id AND s.order_id = k.order_id
)
-- Running and moving aggregates
SELECT *, ROUND(line_total_bucket / SUM(line_total_bucket) OVER (PARTITION BY order_id), 4) AS line_total_bucket_share
FROM verified;
