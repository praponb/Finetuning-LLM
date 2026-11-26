-- Q10b: UNPIVOT (Spark 3.4+) and the stack() equivalent
-- needs setup views: orders
-- @id: Q10b
SELECT customer_id, stack(3, 'Q1', q1_orders, 'Q2', q2_orders, 'Q3', q3_orders) AS (quarter, n_orders)
FROM (
  SELECT
    customer_id,
    SUM(CASE WHEN quarter(order_ts) = 1 THEN 1 ELSE 0 END) AS q1_orders,
    SUM(CASE WHEN quarter(order_ts) = 2 THEN 1 ELSE 0 END) AS q2_orders,
    SUM(CASE WHEN quarter(order_ts) = 3 THEN 1 ELSE 0 END) AS q3_orders
  FROM orders
  GROUP BY customer_id
) m
ORDER BY customer_id, quarter;
