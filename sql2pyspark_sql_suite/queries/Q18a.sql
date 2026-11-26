-- Q18a: Weekly (Monday-aligned) tumbling + 30-day sliding windows over orders
-- needs setup views: orders, order_items
-- @id: Q18a
WITH ord AS (
  SELECT o.order_id, o.order_ts, o.channel, o.status,
         SUM(oi.quantity) AS units
  FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
  GROUP BY o.order_id, o.order_ts, o.channel, o.status
),
weekly AS (
  SELECT
    'weekly' AS grain,
    -- epoch (1970-01-01) is a Thursday, so a 4-day startTime aligns buckets to Monday
    window(order_ts, '1 week', '1 week', '4 days') AS w,
    COUNT(*) AS orders, SUM(units) AS units,
    array_sort(collect_set(channel)) AS channels
  FROM ord
  GROUP BY window(order_ts, '1 week', '1 week', '4 days')
),
sliding AS (
  SELECT
    'sliding_30d_step_15d' AS grain,
    window(order_ts, '30 days', '15 days') AS w,
    COUNT(*) AS orders, SUM(units) AS units,
    array_sort(collect_set(channel)) AS channels
  FROM ord
  WHERE status <> 'CANCELLED'
  GROUP BY window(order_ts, '30 days', '15 days')
)
SELECT
  grain,
  w.start                                AS bucket_start,
  w.end                                  AS bucket_end,
  date_format(w.start, 'EEE')            AS start_weekday,
  orders,
  units,
  channels,
  SUM(orders) OVER (PARTITION BY grain ORDER BY w.start
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_orders
FROM (SELECT * FROM weekly UNION ALL SELECT * FROM sliding)
ORDER BY grain, bucket_start;
