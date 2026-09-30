WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(discount) OVER w_part AS prev_discount,
        LEAD(discount) OVER w_part AS next_discount,
        LAG(shipping_fee) OVER w_part AS prev_shipping_fee,
        LEAD(shipping_fee) OVER w_part AS next_shipping_fee,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM orders
    WHERE order_date >= '2024-02-01'
    WINDOW w_part AS (PARTITION BY customer_id, store_id ORDER BY order_date, id)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        shipping_fee,
        prev_discount,
        next_discount,
        prev_shipping_fee,
        next_shipping_fee,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM with_windows
    WHERE order_date > DATE '2023-07-18'
),
valid_orders AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        shipping_fee,
        prev_discount,
        prev_shipping_fee,
        next_shipping_fee,
        status_title,
        ROUND(shipping_fee, 0) AS shipping_fee_rounded,
        CAST(prev_shipping_fee AS INT) AS prev_shipping_fee_int
    FROM valid
    WHERE store_id IS NOT NULL
      AND prev_shipping_fee < 25
),
latest_match AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.order_date,
        s.prev_discount,
        s.prev_shipping_fee,
        s.next_shipping_fee,
        s.shipping_fee_rounded,
        s.prev_shipping_fee_int,
        x.payment_method,
        x.amount
    FROM valid_orders s
    JOIN LATERAL (
            SELECT c.method AS payment_method, c.amount
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY prev_shipping_fee, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY prev_shipping_fee_int DESC, id) AS seq_no_2
    FROM latest_match
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_method ORDER BY seq_no DESC, id) AS rn
    FROM enriched
    QUALIFY rn = 1
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_item_quantity
    FROM latest s
    JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Join a dimension and aggregate
SELECT
    s.region,
    s.city,
    e.payment_method,
    COUNT(*) AS num_rows,
    MAX(e.amount) AS max_amount
FROM with_last e
INNER JOIN stores s ON e.store_id = s.id
GROUP BY s.region, s.city, e.payment_method;
