WITH with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_item_unit_price
    FROM orders s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.status,
    s.amount,
    s.discount,
    s.shipping_fee,
    s.order_item_unit_price,
    (SELECT AVG(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS avg_account_balance
FROM with_latest s
ORDER BY avg_account_balance DESC NULLS LAST, order_item_unit_price ASC
