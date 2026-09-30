WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        unit_price,
        COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE order_id IN (20, 24, 38)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(unit_price) OVER (PARTITION BY order_id ORDER BY quantity_filled, id) AS next_unit_price,
        LAG(unit_price) OVER (PARTITION BY order_id ORDER BY quantity_filled, id) AS prev_unit_price,
        LAG(quantity_filled) OVER (PARTITION BY order_id ORDER BY quantity_filled, id) AS prev_quantity_filled,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_filled, id) AS row_seq,
        DENSE_RANK() OVER (ORDER BY quantity_filled DESC NULLS FIRST) AS quantity_filled_dense_rank
    FROM filtered
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        unit_price,
        quantity_filled,
        next_unit_price,
        prev_unit_price,
        prev_quantity_filled,
        ROUND(row_seq / unit_price, 2) AS row_seq_per_unit_price,
        CEIL(quantity_filled_dense_rank) AS quantity_filled_dense_rank_whole
    FROM metrics
    WHERE prev_quantity_filled < 49
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC NULLS LAST, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        d.channel,
        d.discount
    FROM ranked s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
joined AS (
    -- Attach orders attributes
    SELECT s.*, b.amount
    FROM with_dims s
    LEFT JOIN orders b
        ON s.order_id = b.id
)
SELECT
    s.id,
    s.order_id,
    s.quantity_filled,
    s.next_unit_price,
    s.latest_rank,
    s.channel,
    s.amount,
    (SELECT SUM(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS sum_order_item_unit_price
FROM joined s
ORDER BY quantity_filled NULLS LAST;
