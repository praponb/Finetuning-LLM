WITH ranked AS (
    SELECT *
    FROM order_items
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC NULLS LAST) = 1
)
SELECT id, unit_price
FROM ranked
ORDER BY id DESC NULLS FIRST, unit_price
LIMIT 20;
