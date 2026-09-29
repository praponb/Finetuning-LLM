WITH valid_orders AS (
    SELECT *
    FROM orders
    WHERE amount < 1120
),
agg AS (
    SELECT status, MIN(shipping_fee) AS min_shipping_fee, MAX(amount) - MIN(amount) AS amount_range
    FROM valid_orders
    GROUP BY status
)
SELECT *
FROM agg
WHERE amount_range >= 23;
