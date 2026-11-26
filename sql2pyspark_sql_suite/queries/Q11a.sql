-- Q11a: Market-basket pairs (self-join), CROSS JOIN calendar, FULL OUTER JOIN
-- needs setup views: products, orders, order_items
-- @id: Q11a
WITH basket AS (
  SELECT DISTINCT oi.order_id, p.category
  FROM order_items oi
  JOIN products p ON p.product_id = oi.product_id
  JOIN orders   o ON o.order_id = oi.order_id
  WHERE o.status <> 'CANCELLED'
),
pairs AS (
  SELECT a.category AS cat_a, b.category AS cat_b, COUNT(DISTINCT a.order_id) AS together
  FROM basket a
  JOIN basket b
    ON a.order_id = b.order_id
   AND a.category < b.category
  GROUP BY a.category, b.category
),
singles AS (
  SELECT category, COUNT(DISTINCT order_id) AS n FROM basket GROUP BY category
),
total AS (SELECT COUNT(DISTINCT order_id) AS n_orders FROM basket)
SELECT
  pr.cat_a, pr.cat_b, pr.together,
  round(pr.together / t.n_orders, 3)                           AS support,
  round(pr.together / sa.n, 3)                                 AS confidence_a_to_b,
  round((pr.together / t.n_orders) / ((sa.n / t.n_orders) * (sb.n / t.n_orders)), 3) AS lift
FROM pairs pr
JOIN singles sa ON sa.category = pr.cat_a
JOIN singles sb ON sb.category = pr.cat_b
CROSS JOIN total t
ORDER BY lift DESC, together DESC;
