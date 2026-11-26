-- Q04a: GROUPING SETS / ROLLUP / CUBE with grouping_id and FILTER clause
-- needs setup views: customers, products, orders, order_items
-- @id: Q04a
WITH base AS (
  SELECT
    c.country_code,
    o.channel,
    p.category,
    o.status,
    oi.quantity,
    oi.quantity * oi.unit_price * (1 - oi.discount_pct) AS net_local
  FROM orders o
  JOIN customers   c  ON c.customer_id = o.customer_id
  JOIN order_items oi ON oi.order_id  = o.order_id
  JOIN products    p  ON p.product_id = oi.product_id
)
SELECT
  CASE WHEN grouping(country_code) = 1 THEN '(all countries)' ELSE country_code END AS country_code,
  CASE WHEN grouping(channel)      = 1 THEN '(all channels)'  ELSE channel      END AS channel,
  CASE WHEN grouping(category)     = 1 THEN '(all categories)' ELSE category    END AS category,
  grouping_id(country_code, channel, category)                                AS gid,
  COUNT(*)                                                                    AS lines,
  SUM(quantity)                                                               AS units,
  SUM(quantity) FILTER (WHERE status = 'DELIVERED')                           AS units_delivered,
  COUNT(*)      FILTER (WHERE status IN ('CANCELLED', 'RETURNED'))            AS lines_lost,
  round(SUM(net_local) FILTER (WHERE status <> 'CANCELLED'), 2)               AS net_local_ex_cancel
FROM base
GROUP BY GROUPING SETS (
  (country_code, channel, category),
  (country_code, channel),
  (country_code),
  (channel, category),
  ()
)
ORDER BY gid, country_code, channel, category;
