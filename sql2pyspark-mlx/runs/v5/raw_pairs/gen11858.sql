WITH cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        ADD_MONTHS(order_date, 6) AS order_date_plus_months
    FROM orders
    WHERE order_date >= DATE '2023-06-07'
      AND channel IN ('web', 'store')
),
not_flagged AS (
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.product_id IN (3, 31)
        )
),
not_flagged_orders AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at < s.order_date_plus_months
        )
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, store_id ORDER BY order_date DESC, id) AS rank_in_group
        FROM not_flagged_orders
    ) x
    WHERE rank_in_group = 1
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.amount,
    s.order_date_plus_months,
    s.rank_in_group,
    (SELECT MAX(o2.shipping_fee) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS max_order_shipping_fee
FROM deduped s
ORDER BY store_id, rank_in_group DESC
