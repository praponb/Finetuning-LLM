WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share,
        LEAD(discount, 2) OVER (PARTITION BY store_id, channel ORDER BY discount, id) AS next_discount
    FROM orders
    WHERE order_date <= '2025-02-03'
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.order_date,
        s.amount,
        s.discount,
        s.amount_share,
        x.quantity
    FROM scored s
    JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored_orders AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(discount, 2) OVER w_ordered AS next_discount,
        ROW_NUMBER() OVER w_recent AS event_seq,
        RANK() OVER (ORDER BY discount DESC) AS discount_rank,
        ROW_NUMBER() OVER (ORDER BY order_date, id) AS seq_no,
        ROW_NUMBER() OVER w_recent AS seq_no_2
    FROM latest_match
    WINDOW w_ordered AS (ORDER BY discount, id), w_recent AS (ORDER BY amount, id)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        amount,
        quantity,
        next_discount,
        event_seq,
        seq_no,
        seq_no_2,
        LEAST(amount_share, seq_no) AS least_amount_share_seq_no,
        ROUND(amount_share / seq_no_2, 2) AS amount_share_per_seq_no_2
    FROM scored_orders
    WHERE order_date >= DATE '2023-06-14'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        amount,
        quantity,
        next_discount,
        seq_no,
        least_amount_share_seq_no,
        amount_share_per_seq_no_2,
        amount_share_per_seq_no_2 * seq_no AS amount_share_per_seq_no_2_times_seq_no
    FROM filtered
    WHERE least_amount_share_seq_no BETWEEN 45 AND 65
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.next_discount,
        latest.order_item_quantity,
        latest.order_item_unit_price
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_orders AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.next_discount,
        s.order_item_quantity,
        latest.payment_amount,
        latest.payment_status
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.amount AS payment_amount, c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN order_item_quantity >= 7 THEN 'HIGH' ELSE 'LOW' END AS order_item_quantity_tier,
        CASE WHEN next_discount >= 0.36 THEN 'HIGH' ELSE 'LOW' END AS next_discount_tier,
        COUNT(DISTINCT payment_status) AS distinct_payment_status,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_last_orders
    GROUP BY CUBE (CASE WHEN order_item_quantity >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN next_discount >= 0.36 THEN 'HIGH' ELSE 'LOW' END)
    HAVING distinct_ids < 1000
)
SELECT
    order_item_quantity_tier,
    next_discount_tier,
    distinct_payment_status
FROM grouped;
