-- Q13b: Consecutive-day activity streaks per customer (web OR order OR ticket)
-- needs setup views: customers, orders, web_events, support_tickets
-- @id: Q13b
WITH activity AS (
  SELECT customer_id, to_date(event_ts)  AS d, 'web'    AS src FROM web_events      WHERE customer_id IS NOT NULL
  UNION ALL
  SELECT customer_id, to_date(order_ts)  AS d, 'order'  AS src FROM orders
  UNION ALL
  SELECT customer_id, to_date(opened_ts) AS d, 'ticket' AS src FROM support_tickets WHERE customer_id IS NOT NULL
),
days AS (
  SELECT customer_id, d, array_sort(collect_set(src)) AS sources
  FROM activity
  GROUP BY customer_id, d
),
grouped AS (
  SELECT
    *,
    date_sub(d, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY d)) AS streak_anchor
  FROM days
),
streaks AS (
  SELECT
    customer_id,
    streak_anchor,
    MIN(d)                                     AS streak_start,
    MAX(d)                                     AS streak_end,
    datediff(MAX(d), MIN(d)) + 1               AS streak_days,
    array_distinct(flatten(collect_list(sources))) AS streak_sources
  FROM grouped
  GROUP BY customer_id, streak_anchor
),
with_gaps AS (
  SELECT
    *,
    datediff(streak_start, LAG(streak_end) OVER (PARTITION BY customer_id ORDER BY streak_start)) - 1 AS idle_days_before
  FROM streaks
)
SELECT
  c.customer_id,
  c.full_name,
  COUNT(w.streak_start)                                                       AS n_streaks,
  coalesce(SUM(w.streak_days), 0)                                             AS active_days,
  MAX(w.streak_days)                                                          AS longest_streak_days,
  -- tie-break: earliest start wins (negative date via datediff for ordering)
  max_by(w.streak_start, struct(w.streak_days, -datediff(w.streak_start, DATE'2000-01-01'))) AS longest_streak_start,
  MAX(w.idle_days_before)                                                     AS longest_idle_gap_days,
  array_sort(array_distinct(flatten(collect_list(w.streak_sources))))         AS all_sources,
  transform(
    array_sort(collect_list(struct(w.streak_start, w.streak_days))),
    s -> concat(date_format(s.streak_start, 'MM-dd'), 'x', s.streak_days)
  )                                                                           AS streak_list,
  count_if(w.streak_days >= 2)                                                AS multi_day_streaks
FROM customers c
LEFT JOIN with_gaps w ON w.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name
ORDER BY c.customer_id;
