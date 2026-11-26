-- Q22b: Tuple IN subquery, correlated AVG comparison, scalar subquery in HAVING
-- needs setup views: products, orders, order_items, fx_rates, web_events
-- @id: Q22b
WITH line_usd AS (
  SELECT oi.order_id, oi.line_no, oi.product_id, p.category, o.customer_id, o.order_ts,
         oi.unit_price * fx.rate_to_usd AS unit_usd
  FROM order_items oi
  JOIN orders o   ON o.order_id = oi.order_id
  JOIN products p ON p.product_id = oi.product_id
  JOIN (SELECT currency, max_by(rate_to_usd, rate_date) AS rate_to_usd FROM fx_rates GROUP BY currency) fx
    ON fx.currency = o.currency
)
SELECT
  l.customer_id,
  COUNT(*)                                          AS premium_lines,
  array_sort(collect_list(l.order_id))              AS premium_orders,
  round(SUM(l.unit_usd), 2)                         AS premium_unit_usd
FROM line_usd l
WHERE
  -- bought above the category's average unit price (correlated)
  l.unit_usd > (SELECT AVG(l2.unit_usd) FROM line_usd l2 WHERE l2.category = l.category)
  -- and the order came after a same-day checkout event (tuple IN)
  AND (l.customer_id, to_date(l.order_ts)) IN (
        SELECT customer_id, to_date(event_ts) FROM web_events WHERE event_type = 'checkout')
GROUP BY l.customer_id
HAVING COUNT(*) >= (SELECT MIN(cnt) FROM (SELECT COUNT(*) AS cnt FROM order_items GROUP BY order_id))
ORDER BY l.customer_id;
