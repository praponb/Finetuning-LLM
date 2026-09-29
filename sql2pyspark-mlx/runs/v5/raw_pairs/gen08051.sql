WITH valid_orders AS (
    SELECT *
    FROM orders
    WHERE store_id IS NULL AND (discount <= 0.15 OR store_id IS NOT NULL)
),
summary AS (
    SELECT channel, status, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM valid_orders
    GROUP BY channel, status
)
SELECT *
FROM summary;
