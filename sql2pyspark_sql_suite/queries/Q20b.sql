-- Q20b: Histogram with width_bucket + window over an aggregate in the same SELECT
-- needs setup views: orders, order_items, fx_rates
-- @id: Q20b
WITH fx AS (
  SELECT order_id, rate_to_usd FROM (
    SELECT o.order_id, f.rate_to_usd,
           ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY f.rate_date DESC) AS rn
    FROM orders o JOIN fx_rates f ON f.currency = o.currency AND f.rate_date <= to_date(o.order_ts)
  ) WHERE rn = 1
),
order_usd AS (
  SELECT o.order_id, o.status,
         CAST(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * fx.rate_to_usd) AS DOUBLE) AS usd
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  JOIN fx ON fx.order_id = o.order_id
  GROUP BY o.order_id, o.status
)
SELECT
  width_bucket(usd, 0, 200, 4)                                                   AS bucket,
  CASE width_bucket(usd, 0, 200, 4)
    WHEN 0 THEN '< 0' WHEN 5 THEN '>= 200'
    ELSE concat('[', (width_bucket(usd, 0, 200, 4) - 1) * 50, ', ', width_bucket(usd, 0, 200, 4) * 50, ')')
  END                                                                            AS bucket_label,
  COUNT(*)                                                                       AS orders,
  round(SUM(usd), 2)                                                             AS usd,
  round(MIN(usd), 2)                                                             AS min_usd,
  round(MAX(usd), 2)                                                             AS max_usd,
  count_if(status IN ('CANCELLED', 'RETURNED'))                                  AS lost_orders,
  SUM(COUNT(*)) OVER (ORDER BY width_bucket(usd, 0, 200, 4))                     AS cum_orders,
  round(100.0 * SUM(COUNT(*)) OVER (ORDER BY width_bucket(usd, 0, 200, 4))
        / SUM(COUNT(*)) OVER (), 1)                                              AS cum_pct,
  round(100 * SUM(usd) / SUM(SUM(usd)) OVER (), 2)                               AS pct_of_usd,
  RANK() OVER (ORDER BY COUNT(*) DESC, width_bucket(usd, 0, 200, 4))             AS popularity_rank
FROM order_usd
GROUP BY width_bucket(usd, 0, 200, 4)
ORDER BY bucket;
