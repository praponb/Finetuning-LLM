-- Q20a: Distribution stats per category x channel (GROUP BY ALL)
-- needs setup views: products, orders, order_items, fx_rates
-- @id: Q20a
WITH fx AS (
  SELECT order_id, rate_to_usd FROM (
    SELECT o.order_id, f.rate_to_usd,
           ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY f.rate_date DESC) AS rn
    FROM orders o JOIN fx_rates f ON f.currency = o.currency AND f.rate_date <= to_date(o.order_ts)
  ) WHERE rn = 1
),
lines AS (
  SELECT
    p.category, o.channel, o.customer_id, o.order_id, oi.quantity, oi.discount_pct,
    CAST(oi.unit_price * fx.rate_to_usd AS DOUBLE)                                       AS unit_usd,
    CAST(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * fx.rate_to_usd AS DOUBLE)  AS net_usd
  FROM order_items oi
  JOIN orders o   ON o.order_id   = oi.order_id
  JOIN products p ON p.product_id = oi.product_id
  JOIN fx         ON fx.order_id  = oi.order_id
)
SELECT
  category,
  channel,
  COUNT(*)                                                         AS n_lines,
  COUNT(DISTINCT customer_id, order_id)                            AS distinct_cust_orders,
  approx_count_distinct(customer_id)                               AS approx_customers,
  round(SUM(net_usd), 2)                                           AS net_usd,
  round(AVG(DISTINCT quantity), 3)                                 AS avg_distinct_qty,
  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY net_usd), 4)   AS p50_cont,
  round(percentile_disc(0.5) WITHIN GROUP (ORDER BY net_usd), 4)   AS p50_disc,
  round(percentile_cont(0.9) WITHIN GROUP (ORDER BY net_usd DESC), 4) AS p10_via_desc,
  round(median(net_usd), 4)                                        AS median_usd,
  transform(percentile(net_usd, array(0.25, 0.75)), x -> round(x, 4)) AS iqr_exact,
  round(stddev_pop(net_usd), 4)                                    AS sd_pop,
  round(var_samp(net_usd), 4)                                      AS var_samp,
  round(corr(quantity, unit_usd), 4)                               AS corr_qty_price,
  round(covar_samp(quantity, net_usd), 4)                          AS covar_qty_net,
  round(regr_slope(net_usd, quantity), 4)                          AS slope_net_per_unit,
  regr_count(net_usd, quantity)                                    AS regr_n,
  every(discount_pct = 0)                                          AS never_discounted,
  some(discount_pct >= 0.2)                                        AS any_deep_discount
FROM lines
GROUP BY ALL
ORDER BY category, channel;
