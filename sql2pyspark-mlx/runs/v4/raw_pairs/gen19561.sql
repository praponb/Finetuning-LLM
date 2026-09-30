WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.amount,
        d.order_date,
        d.channel
    FROM order_items e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date DESC, id) AS latest_rank
    FROM with_dims
    QUALIFY latest_rank = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS dedup_rank
        FROM deduped
    ) t
    WHERE dedup_rank = 1
),
with_dims_order_items AS (
    -- Attach products attributes
    SELECT src.*, d.product_name
    FROM ranked src
    LEFT JOIN products d
        ON src.product_id = d.id
)
-- Running and moving aggregates
SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    amount,
    order_date,
    channel,
    latest_rank,
    dedup_rank,
    product_name,
    LEAD(unit_price, 1) OVER w_ordered AS next_unit_price,
    ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY channel), 4) AS unit_price_share,
    CORR(quantity, unit_price) OVER (PARTITION BY order_id) AS corr_quantity_unit_price,
    AVG(unit_price) OVER (PARTITION BY product_id ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_avg_2
FROM with_dims_order_items
WINDOW w_ordered AS (PARTITION BY order_id ORDER BY dedup_rank, id);
