-- Q08: Point-in-time FX conversion + time-range window frames
-- needs setup views: orders, order_items, fx_rates
-- @id: Q08
WITH order_totals AS (
  SELECT
    o.order_id, o.customer_id, o.order_ts, o.currency, o.status, o.channel,
    SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct)) AS net_local
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  GROUP BY o.order_id, o.customer_id, o.order_ts, o.currency, o.status, o.channel
),
fx_pit AS (
  -- latest rate with rate_date <= order date (as-of join)
  SELECT * FROM (
    SELECT /*+ BROADCAST(f) */
      ot.order_id,
      f.rate_to_usd,
      f.rate_date,
      ROW_NUMBER() OVER (PARTITION BY ot.order_id ORDER BY f.rate_date DESC) AS rn
    FROM order_totals ot
    JOIN fx_rates f
      ON f.currency = ot.currency
     AND f.rate_date <= to_date(ot.order_ts)
  ) z WHERE rn = 1
),
usd AS (
  SELECT
    ot.*,
    fx.rate_date                               AS fx_rate_date,
    CAST(ot.net_local * fx.rate_to_usd AS DECIMAL(18,2)) AS net_usd
  FROM order_totals ot
  LEFT JOIN fx_pit fx ON fx.order_id = ot.order_id
)
SELECT
  order_id, customer_id, order_ts, currency, fx_rate_date, status, net_usd,
  SUM(net_usd) OVER (
    ORDER BY order_ts
    RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
  )                                                                     AS rolling_30d_usd,
  COUNT(*) OVER (
    PARTITION BY customer_id ORDER BY order_ts
    RANGE BETWEEN INTERVAL 60 DAYS PRECEDING AND CURRENT ROW
  )                                                                     AS cust_orders_60d,
  SUM(net_usd) OVER (
    PARTITION BY customer_id ORDER BY order_ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )                                                                     AS cust_running_usd,
  AVG(net_usd) OVER (
    PARTITION BY channel ORDER BY order_ts
    ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
  )                                                                     AS channel_centered_avg5,
  LAST_VALUE(CASE WHEN status = 'DELIVERED' THEN order_id END) IGNORE NULLS OVER (
    PARTITION BY customer_id ORDER BY order_ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  )                                                                     AS prev_delivered_order,
  FIRST_VALUE(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts) AS first_order_ts,
  datediff(
    to_date(order_ts),
    to_date(LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts))
  )                                                                     AS days_since_prev,
  LEAD(status, 1, 'NONE') OVER (PARTITION BY customer_id ORDER BY order_ts) AS next_status,
  DENSE_RANK() OVER (PARTITION BY date_trunc('MONTH', order_ts) ORDER BY net_usd DESC) AS month_rank
FROM usd
ORDER BY order_ts;
