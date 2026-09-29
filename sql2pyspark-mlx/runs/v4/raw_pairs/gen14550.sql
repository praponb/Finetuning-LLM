WITH orders_by_store AS (
    SELECT store_id, MIN(shipping_fee) AS min_shipping_fee
    FROM orders
    WHERE channel = 'web'
    GROUP BY store_id
)
SELECT s.id, s.sqft, obs.min_shipping_fee
FROM stores s
INNER JOIN orders_by_store obs ON s.id = obs.store_id
ORDER BY id DESC;
