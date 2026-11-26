-- Q05c: Arrays, maps, structs and higher-order functions; LATERAL VIEW explode
-- needs setup views: orders
-- @id: Q05c
SELECT customer_id, o.*
FROM (
  SELECT customer_id,
         array(
           named_struct('k', 'first_order',  'v', CAST(MIN(order_ts) AS STRING)),
           named_struct('k', 'last_order',   'v', CAST(MAX(order_ts) AS STRING)),
           named_struct('k', 'order_count',  'v', CAST(COUNT(*) AS STRING))
         ) AS kv
  FROM orders
  GROUP BY customer_id
) x
LATERAL VIEW inline(x.kv) o AS k, v
ORDER BY customer_id, k;
