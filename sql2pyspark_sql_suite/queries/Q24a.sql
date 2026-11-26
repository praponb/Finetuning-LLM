-- Q24a: PIVOT on (channel, status) tuples with two aggregates
-- needs setup views: customers, orders, order_items
-- @id: Q24a
SELECT *
FROM (
  -- only the columns PIVOT should group by may appear here (country_code)
  SELECT c.country_code, o.channel, o.status,
         oi.quantity * oi.unit_price * (1 - oi.discount_pct) AS net_local
  FROM orders o
  JOIN customers c    ON c.customer_id = o.customer_id
  JOIN order_items oi ON oi.order_id = o.order_id
) src
PIVOT (
  COUNT(*)                AS lines,
  round(SUM(net_local), 0) AS net
  FOR (channel, status) IN (
    ('web',    'DELIVERED') AS web_ok,
    ('app',    'DELIVERED') AS app_ok,
    ('market', 'DELIVERED') AS mkt_ok,
    ('web',    'CANCELLED') AS web_cxl,
    ('app',    'RETURNED')  AS app_ret,
    ('app',    'SHIPPED')   AS app_ship
  )
)
ORDER BY country_code;
