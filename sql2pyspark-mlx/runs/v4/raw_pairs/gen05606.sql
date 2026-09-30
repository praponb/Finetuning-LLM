WITH with_last AS (
    SELECT
        s.*,
        x.order_item_unit_price,
        x.order_item_quantity
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    order_date,
    status,
    channel,
    amount,
    shipping_fee,
    order_item_unit_price,
    order_item_quantity
FROM with_last
ORDER BY shipping_fee DESC NULLS LAST;
