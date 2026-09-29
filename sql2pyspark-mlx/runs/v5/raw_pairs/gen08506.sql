WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM orders
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY amount DESC NULLS LAST) = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC NULLS LAST, id) = 1
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.order_date,
        s.status,
        s.amount,
        x.order_item_quantity
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_date,
        order_item_quantity,
        UPPER(status) AS status_upper
    FROM latest_match
    WHERE order_date >= DATE '2023-07-26'
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.paid_at,
        top1.amount
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.paid_at, c.amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Window metrics over several different partitions
SELECT
    id,
    paid_at,
    amount,
    LEAD(amount) OVER (ORDER BY order_item_quantity, id) AS next_amount,
    LAG(amount) OVER (ORDER BY order_item_quantity, id) AS prev_amount,
    LEAD(order_item_quantity) OVER (ORDER BY order_item_quantity, id) AS next_order_item_quantity,
    LAG(order_item_quantity) OVER (ORDER BY order_item_quantity, id) AS prev_order_item_quantity,
    ROW_NUMBER() OVER (ORDER BY order_item_quantity, id) AS event_seq,
    LAG(order_item_quantity, 1) OVER (ORDER BY order_date, id) AS prev_order_item_quantity_2
FROM with_latest
