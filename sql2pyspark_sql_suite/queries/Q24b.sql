-- Q24b: UNPIVOT two value columns at once, keeping NULLs
-- needs setup views: orders, order_items
-- @id: Q24b
WITH wide AS (
  SELECT
    o.customer_id,
    COUNT(DISTINCT CASE WHEN quarter(o.order_ts) = 1 THEN o.order_id END)                 AS q1_orders,
    SUM(CASE WHEN quarter(o.order_ts) = 1 THEN oi.quantity END)                           AS q1_units,
    COUNT(DISTINCT CASE WHEN quarter(o.order_ts) = 2 THEN o.order_id END)                 AS q2_orders,
    SUM(CASE WHEN quarter(o.order_ts) = 2 THEN oi.quantity END)                           AS q2_units,
    COUNT(DISTINCT CASE WHEN quarter(o.order_ts) = 3 THEN o.order_id END)                 AS q3_orders,
    SUM(CASE WHEN quarter(o.order_ts) = 3 THEN oi.quantity END)                           AS q3_units
  FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
  GROUP BY o.customer_id
)
SELECT customer_id, quarter, n_orders, n_units,
       n_units IS NULL                                                   AS no_units,
       SUM(n_units) OVER (PARTITION BY customer_id ORDER BY quarter)    AS cum_units
FROM wide
UNPIVOT INCLUDE NULLS (
  (n_orders, n_units) FOR quarter IN (
    (q1_orders, q1_units) AS Q1,
    (q2_orders, q2_units) AS Q2,
    (q3_orders, q3_units) AS Q3
  )
)
ORDER BY customer_id, quarter;
