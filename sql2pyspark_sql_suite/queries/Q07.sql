-- Q07: Set operations
-- needs setup views: orders, web_events
-- @id: Q07
WITH web_buyers  AS (SELECT DISTINCT customer_id FROM orders WHERE channel = 'web'),
     app_buyers  AS (SELECT DISTINCT customer_id FROM orders WHERE channel = 'app'),
     browsers    AS (SELECT DISTINCT customer_id FROM web_events WHERE customer_id IS NOT NULL)
SELECT 'web_and_app'        AS cohort, customer_id FROM (SELECT customer_id FROM web_buyers INTERSECT SELECT customer_id FROM app_buyers)
UNION ALL
SELECT 'web_only'           AS cohort, customer_id FROM (SELECT customer_id FROM web_buyers EXCEPT SELECT customer_id FROM app_buyers)
UNION ALL
SELECT 'app_only'           AS cohort, customer_id FROM (SELECT customer_id FROM app_buyers EXCEPT SELECT customer_id FROM web_buyers)
UNION ALL
SELECT 'browsed_no_purchase' AS cohort, customer_id FROM (
  SELECT customer_id FROM browsers
  EXCEPT
  SELECT customer_id FROM orders WHERE status IN ('DELIVERED', 'SHIPPED')
)
UNION
SELECT 'any_activity'       AS cohort, customer_id FROM (
  -- inner UNION ALL keeps duplicates on purpose: only the outer UNION removes them
  SELECT customer_id FROM browsers UNION ALL SELECT customer_id FROM orders
)
ORDER BY cohort, customer_id;
