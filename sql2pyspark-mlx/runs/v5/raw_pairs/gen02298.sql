WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee DESC NULLS LAST, id) AS row_num
    FROM orders
    QUALIFY row_num = 1
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.status,
    s.shipping_fee,
    s.row_num,
    (SELECT SUM(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS sum_inventory_quantity
FROM latest s
ORDER BY store_id DESC NULLS LAST
