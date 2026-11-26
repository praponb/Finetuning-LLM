-- Q24c: Transpose: metrics-as-columns -> stack() -> PIVOT channels to columns
-- needs setup views: customers, orders, order_items
-- @id: Q24c
WITH by_channel AS (
  SELECT
    o.channel,
    CAST(COUNT(DISTINCT o.order_id) AS DOUBLE)                            AS orders,
    CAST(SUM(oi.quantity) AS DOUBLE)                                      AS units,
    CAST(COUNT(DISTINCT o.customer_id) AS DOUBLE)                         AS customers,
    -- stack() needs every value column to have the same type
    CAST(round(AVG(oi.discount_pct) * 100, 2) AS DOUBLE)                  AS avg_disc_pct,
    CAST(round(count_if(o.status IN ('CANCELLED','RETURNED')) / COUNT(*) * 100, 2) AS DOUBLE) AS lost_line_pct
  FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
  GROUP BY o.channel
),
long_fmt AS (
  SELECT channel,
         stack(5, 'orders', orders, 'units', units, 'customers', customers,
                  'avg_disc_pct', avg_disc_pct, 'lost_line_pct', lost_line_pct) AS (metric, value)
  FROM by_channel
)
SELECT *
FROM long_fmt
PIVOT (first(value) FOR channel IN ('web' AS web, 'app' AS app, 'market' AS market))
ORDER BY metric;
