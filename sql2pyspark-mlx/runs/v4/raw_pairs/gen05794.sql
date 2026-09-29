WITH valid AS (
    SELECT *, DATE_TRUNC('MONTH', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-11-07')
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_period DESC, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity NULLS FIRST, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
latest_inventory AS (
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY rn DESC, id) = 1
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.quantity,
    s.last_restock_period,
    s.rn,
    s.latest_rank,
    (SELECT MIN(x.shipping_fee) FROM orders x WHERE x.store_id = s.store_id) AS min_order_shipping_fee
FROM latest_inventory s
ORDER BY min_order_shipping_fee, store_id;
