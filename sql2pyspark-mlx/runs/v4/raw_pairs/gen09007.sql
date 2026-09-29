WITH joined AS (
    SELECT
        src.*,
        p.launch_date,
        p.brand,
        p.price
    FROM order_items src
    LEFT JOIN products p
        ON src.product_id = p.id
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.unit_price,
    s.launch_date,
    s.brand,
    (SELECT AVG(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS avg_inventory_quantity
FROM joined s
