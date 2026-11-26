-- Q25b: SQL pipe syntax
-- needs setup views: orders, order_items
-- requires Spark 4.x
-- @id: Q25b
FROM orders AS o
|> JOIN order_items AS oi ON oi.order_id = o.order_id
|> WHERE o.status <> 'CANCELLED'
|> EXTEND oi.quantity * oi.unit_price * (1 - oi.discount_pct) AS net_local,
          date_format(o.order_ts, 'yyyy-MM')                   AS ym
|> AGGREGATE COUNT(DISTINCT o.order_id) AS orders, round(SUM(net_local), 2) AS net_local, SUM(oi.quantity) AS units
   GROUP BY o.currency AS currency, ym
|> EXTEND round(net_local / orders, 2) AS avg_order_local
|> WHERE units > 1
|> SELECT currency, ym, orders, units, net_local, avg_order_local
|> ORDER BY currency, ym;
