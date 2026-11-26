-- Q09a: Funnel analysis per customer-day
-- needs setup views: orders, web_events
-- @id: Q09a
WITH ev AS (
  SELECT customer_id, to_date(event_ts) AS d, event_ts, event_type
  FROM web_events
  WHERE customer_id IS NOT NULL
),
funnel AS (
  SELECT
    customer_id,
    d,
    MIN(CASE WHEN event_type IN ('page_view','search') THEN event_ts END) AS t_entry,
    MIN(CASE WHEN event_type = 'product_view'  THEN event_ts END)         AS t_view,
    MIN(CASE WHEN event_type = 'add_to_cart'   THEN event_ts END)         AS t_cart,
    MIN(CASE WHEN event_type = 'checkout'      THEN event_ts END)         AS t_checkout,
    concat_ws(' > ', array_distinct(collect_list(event_type)))            AS path
  FROM ev
  GROUP BY customer_id, d
)
SELECT
  f.customer_id,
  f.d,
  f.path,
  CASE
    WHEN f.t_checkout IS NOT NULL THEN '4_checkout'
    WHEN f.t_cart     IS NOT NULL THEN '3_cart'
    WHEN f.t_view     IS NOT NULL THEN '2_view'
    ELSE '1_entry'
  END                                                                    AS deepest_stage,
  (unix_timestamp(f.t_checkout) - unix_timestamp(coalesce(f.t_entry, f.t_view))) / 60 AS minutes_to_checkout,
  o.order_id                                                             AS matched_order,
  array_contains(split(f.path, ' > '), 'search')                         AS used_search
FROM funnel f
LEFT JOIN orders o
  ON o.customer_id = f.customer_id
 AND o.order_ts BETWEEN f.t_checkout AND f.t_checkout + INTERVAL 10 MINUTES
ORDER BY f.customer_id, f.d;
