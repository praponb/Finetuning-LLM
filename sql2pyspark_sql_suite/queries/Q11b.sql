-- Q11b: Market-basket pairs (self-join), CROSS JOIN calendar, FULL OUTER JOIN
-- needs setup views: orders
-- @id: Q11b
WITH months AS (
  SELECT explode(sequence(DATE'2024-01-01', DATE'2024-08-01', INTERVAL 1 MONTH)) AS month_start
),
channels AS (SELECT DISTINCT channel FROM orders),
spine AS (SELECT m.month_start, c.channel FROM months m CROSS JOIN channels c),
actuals AS (
  SELECT CAST(date_trunc('MONTH', order_ts) AS DATE) AS month_start, channel, COUNT(*) AS orders
  FROM orders
  GROUP BY 1, 2
)
SELECT
  coalesce(s.month_start, a.month_start) AS month_start,
  coalesce(s.channel, a.channel)         AS channel,
  coalesce(a.orders, 0)                  AS orders,
  s.month_start IS NULL                  AS outside_spine
FROM spine s
FULL OUTER JOIN actuals a
  ON a.month_start = s.month_start AND a.channel = s.channel
ORDER BY month_start, channel;
