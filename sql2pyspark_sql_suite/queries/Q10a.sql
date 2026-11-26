-- Q10a: UNPIVOT (Spark 3.4+) and the stack() equivalent
-- needs setup views: orders
-- @id: Q10a
WITH monthly AS (
  SELECT
    customer_id,
    SUM(CASE WHEN month(order_ts) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS q1_orders,
    SUM(CASE WHEN month(order_ts) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS q2_orders,
    SUM(CASE WHEN month(order_ts) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS q3_orders
  FROM orders
  GROUP BY customer_id
)
SELECT customer_id, quarter, n_orders
FROM monthly
UNPIVOT EXCLUDE NULLS (n_orders FOR quarter IN (q1_orders AS Q1, q2_orders AS Q2, q3_orders AS Q3))
WHERE n_orders > 0
ORDER BY customer_id, quarter;
