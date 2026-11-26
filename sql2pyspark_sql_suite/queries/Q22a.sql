-- Q22a: Chained correlated LATERAL subqueries, EXISTS in SELECT / CASE, nested CTE
-- needs setup views: customers, products, orders, order_items, web_events, support_tickets
-- @id: Q22a
SELECT
  c.customer_id,
  c.full_name,
  agg.n_orders,
  agg.last_order_ts,
  agg.n_channels,
  derived.orders_per_year,
  EXISTS (SELECT 1 FROM support_tickets t WHERE t.customer_id = c.customer_id)          AS has_ticket,
  CASE
    WHEN EXISTS (SELECT 1 FROM support_tickets t
                  WHERE t.customer_id = c.customer_id AND t.priority = 'P1')            THEN 'escalated'
    WHEN NOT EXISTS (SELECT 1 FROM web_events w WHERE w.customer_id = c.customer_id)     THEN 'offline'
    ELSE 'normal'
  END                                                                                   AS care_flag,
  top_cat.category                                                                      AS top_category
FROM customers c,
LATERAL (
  SELECT COUNT(*) AS n_orders, MAX(o.order_ts) AS last_order_ts, COUNT(DISTINCT o.channel) AS n_channels
  FROM orders o
  WHERE o.customer_id = c.customer_id
) agg,
LATERAL (
  SELECT round(agg.n_orders / greatest(months_between(DATE'2024-08-01', c.signup_date) / 12, 0.5), 3) AS orders_per_year
) derived
LEFT JOIN (
  -- CTE nested inside a subquery
  WITH cat_units AS (
    SELECT o.customer_id, p.category, SUM(oi.quantity) AS units
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    JOIN products p     ON p.product_id = oi.product_id
    GROUP BY o.customer_id, p.category
  )
  SELECT customer_id, max_by(category, struct(units, category)) AS category
  FROM cat_units
  GROUP BY customer_id
) top_cat ON top_cat.customer_id = c.customer_id
ORDER BY c.customer_id;
