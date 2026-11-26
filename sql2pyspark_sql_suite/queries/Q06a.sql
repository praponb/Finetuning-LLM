-- Q06a: Semi/anti joins and correlated subqueries
-- needs setup views: customers, products, orders, order_items, web_events
-- @id: Q06a
SELECT
  c.customer_id,
  c.full_name,
  c.country_code,
  (SELECT MAX(oi.quantity * oi.unit_price)
     FROM orders o2
     JOIN order_items oi ON oi.order_id = o2.order_id
    WHERE o2.customer_id = c.customer_id)                                   AS max_line_local,
  (SELECT COUNT(*) FROM web_events w WHERE w.customer_id = c.customer_id)   AS web_events
FROM customers c
LEFT SEMI JOIN (
  SELECT o.customer_id
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  JOIN products p     ON p.product_id = oi.product_id
  WHERE p.category = 'electronics'
) elec ON elec.customer_id = c.customer_id
WHERE NOT EXISTS (
        SELECT 1 FROM orders ox
        WHERE ox.customer_id = c.customer_id
          AND ox.status IN ('CANCELLED', 'RETURNED')
      )
ORDER BY c.customer_id;
