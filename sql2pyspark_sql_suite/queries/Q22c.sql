-- Q22c: Multiset set operations: INTERSECT ALL and EXCEPT ALL
-- needs setup views: customers, products, orders, order_items
-- @id: Q22c
WITH bought AS (            -- one row per unit-line, duplicates matter
  SELECT o.customer_id, p.category
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  JOIN products p     ON p.product_id = oi.product_id
  WHERE o.status <> 'CANCELLED'
),
wanted AS (               -- interests, doubled for tiers that get two recommendations
  SELECT c.customer_id, i AS category
  FROM customers c LATERAL VIEW explode(c.interests) e AS i
  UNION ALL
  SELECT c.customer_id, i
  FROM customers c LATERAL VIEW explode(c.interests) e AS i
  WHERE c.tier IN ('GOLD', 'PLATINUM')
)
SELECT 'bought_and_wanted' AS bucket, customer_id, category FROM (SELECT * FROM bought INTERSECT ALL SELECT * FROM wanted)
UNION ALL
SELECT 'wanted_not_bought' AS bucket, customer_id, category FROM (SELECT * FROM wanted EXCEPT ALL SELECT * FROM bought)
UNION ALL
SELECT 'bought_not_wanted' AS bucket, customer_id, category FROM (SELECT * FROM bought EXCEPT ALL SELECT * FROM wanted)
UNION ALL
SELECT 'distinct_overlap'  AS bucket, customer_id, category FROM (SELECT * FROM bought INTERSECT SELECT * FROM wanted)
ORDER BY bucket, customer_id, category;
