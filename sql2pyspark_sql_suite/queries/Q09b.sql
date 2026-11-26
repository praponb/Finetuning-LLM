-- Q09b: Funnel analysis per customer-day
-- needs setup views: web_events
-- @id: Q09b
WITH stage_flags AS (
  SELECT
    customer_id, to_date(event_ts) AS d,
    max(event_type = 'product_view') AS viewed,
    max(event_type = 'add_to_cart')  AS carted,
    max(event_type = 'checkout')     AS checked_out
  FROM web_events
  WHERE customer_id IS NOT NULL
  GROUP BY customer_id, to_date(event_ts)
)
SELECT
  COUNT(*)                                                     AS visit_days,
  count_if(viewed)                                             AS view_days,
  count_if(carted)                                             AS cart_days,
  count_if(checked_out)                                        AS checkout_days,
  round(try_divide(count_if(carted), count_if(viewed)) * 100, 1)       AS view_to_cart_pct,
  round(try_divide(count_if(checked_out), count_if(carted)) * 100, 1)  AS cart_to_checkout_pct
FROM stage_flags;
