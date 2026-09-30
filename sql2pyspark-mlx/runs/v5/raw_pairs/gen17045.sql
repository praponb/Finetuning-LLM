WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(quantity) AS quantity_whole
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        o.order_date,
        o.channel
    FROM valid s
    LEFT JOIN orders o
        ON s.order_id = o.id
    WHERE o.customer_id IN (4, 38)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        quantity_whole,
        channel,
        LEAD(quantity_whole) OVER w_ordered AS next_quantity_whole,
        LAG(unit_price) OVER w_ordered AS prev_unit_price,
        LEAD(unit_price) OVER w_ordered AS next_unit_price,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY channel, order_id ORDER BY quantity_whole ASC, id ASC)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY next_unit_price ASC) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.channel,
        s.next_quantity_whole,
        s.prev_unit_price,
        s.next_unit_price,
        s.row_seq,
        d.amount,
        d.status
    FROM ranked s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
valid_order_items AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        channel,
        prev_unit_price,
        next_unit_price,
        row_seq,
        amount,
        ROUND(next_quantity_whole * 1.1, 2) AS next_quantity_whole_scaled,
        CAST(next_unit_price AS INT) AS next_unit_price_int,
        COALESCE(status, 'NONE') AS status_filled
    FROM joined
    WHERE row_seq BETWEEN 5 AND 6
),
ranked_order_items AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, order_id ORDER BY prev_unit_price DESC NULLS FIRST, id ASC) AS latest_rank
        FROM valid_order_items
    ) x
    WHERE latest_rank = 1
)
SELECT
    s.id,
    s.order_id,
    s.channel,
    s.prev_unit_price,
    s.next_quantity_whole_scaled,
    s.next_unit_price_int,
    s.status_filled,
    s.latest_rank,
    (SELECT SUM(t2.quantity) FROM order_items t2 WHERE t2.order_id = s.order_id) AS sum_order_item_quantity
FROM ranked_order_items s
