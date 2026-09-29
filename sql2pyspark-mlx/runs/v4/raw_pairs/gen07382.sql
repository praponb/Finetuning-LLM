WITH valid_shipments AS (
    SELECT *
    FROM shipments
    WHERE order_id = 30
),
Agg AS (
    SELECT order_id, YEAR(shipped_date) AS shipped_year, MIN(cost) AS min_cost
    FROM valid_shipments
    GROUP BY order_id, YEAR(shipped_date)
)
SELECT order_id, shipped_year
FROM Agg
WHERE min_cost >= 93
ORDER BY order_id DESC;
