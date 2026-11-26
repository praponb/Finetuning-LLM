-- Q26 part 2 of 2: THE BIG ONE #2: order margin control tower
-- too long to convert in one go (3,424 tokens), so split by CTE; run parts 1-2 in order
-- needs temp views from earlier parts: cat_path, fx, line_margin, scd, order_tickets, pay
-- returns the result of Q26
-- @id: Q26_part2
WITH
-- 12. Order-level fact
order_fact AS (
  SELECT /*+ SHUFFLE_HASH(lm) */
    o.order_id, o.customer_id, o.order_ts, o.status, o.channel, o.currency,
    c.country_code,
    coalesce(s.tier, 'UNKNOWN')                                            AS tier_at_order,
    SUM(lm.net_usd)                                                        AS net_usd,
    SUM(lm.cogs_usd)                                                       AS cogs_usd,
    SUM(lm.discount_usd)                                                   AS discount_usd,
    SUM(lm.quantity)                                                       AS units,
    bool_or(lm.cost_is_estimate)                                           AS has_estimated_cost,
    bool_or(lm.any_wh_out)                                                 AS has_stockout_product,
    array_sort(collect_set(lm.division))                                   AS divisions,
    max_by(lm.cat_path, struct(lm.net_usd, lm.line_no))                    AS main_cat_path
  FROM orders o
  JOIN customers c   ON c.customer_id = o.customer_id
  JOIN line_margin lm ON lm.order_id = o.order_id
  LEFT JOIN scd s
    ON s.customer_id = o.customer_id
   AND to_date(o.order_ts) >= s.valid_from AND to_date(o.order_ts) < s.valid_to
  GROUP BY o.order_id, o.customer_id, o.order_ts, o.status, o.channel, o.currency, c.country_code, s.tier
),
-- 13. Margin after status effects (cancelled = 0 revenue, returned = refund, cost kept)
order_pl AS (
  SELECT
    f.*,
    fx.rate_to_usd,
    CASE f.status WHEN 'CANCELLED' THEN 0 ELSE f.net_usd END               AS rev_usd,
    CASE f.status WHEN 'CANCELLED' THEN 0 ELSE f.cogs_usd END              AS cost_usd,
    CAST(coalesce(p.refunded_local, 0) * fx.rate_to_usd AS DECIMAL(18,4))   AS refund_usd,
    CAST(coalesce(p.captured_local, 0) * fx.rate_to_usd AS DECIMAL(18,4))   AS captured_usd
  FROM order_fact f
  JOIN fx ON fx.order_id = f.order_id
  LEFT JOIN pay p ON p.order_id = f.order_id
),
-- 14. Margin metrics
order_margin AS (
  SELECT
    pl.*,
    CAST(pl.rev_usd - pl.cost_usd - CASE WHEN pl.status = 'RETURNED' THEN pl.refund_usd ELSE 0 END
         AS DECIMAL(18,2))                                                 AS gross_margin_usd,
    round(try_divide(pl.rev_usd - pl.cost_usd
                     - CASE WHEN pl.status = 'RETURNED' THEN pl.refund_usd ELSE 0 END,
                     pl.rev_usd) * 100, 2)                                  AS margin_pct,
    coalesce(t.tickets, 0)                                                 AS tickets,
    t.worst_priority,
    coalesce(t.has_open_ticket, false)                                     AS has_open_ticket,
    coalesce(t.ticket_cost_risk, false)                                    AS ticket_cost_risk,
    coalesce(t.sla_breaches, 0)                                            AS sla_breaches,
    t.avg_csat
  FROM order_pl pl
  LEFT JOIN order_tickets t ON t.order_id = pl.order_id
),
-- 15. Flags as an array (filter out the non-firing ones)
flagged AS (
  SELECT
    m.*,
    filter(array(
      CASE WHEN m.margin_pct < 30                        THEN 'LOW_MARGIN'  END,
      CASE WHEN m.has_estimated_cost                     THEN 'EST_COST'    END,
      CASE WHEN m.has_stockout_product                   THEN 'STOCK_RISK'  END,
      CASE WHEN m.ticket_cost_risk                       THEN 'TICKET_RISK' END,
      CASE WHEN m.sla_breaches > 0                       THEN 'SLA_BREACH'  END,
      CASE WHEN m.discount_usd > 0.1 * m.net_usd         THEN 'DEEP_DISC'   END,
      CASE WHEN m.status IN ('DELIVERED', 'SHIPPED')
            AND abs(m.captured_usd - m.net_usd) > 1      THEN 'PAY_MISMATCH' END,
      CASE WHEN m.tier_at_order = 'UNKNOWN'              THEN 'NO_TIER'     END
    ), x -> x IS NOT NULL)                                                 AS flags
  FROM order_margin m
),
-- 16. Scores and windows
scored AS (
  SELECT
    f.*,
    size(f.flags)                                                          AS n_flags,
    round(
        0.5 * coalesce(PERCENT_RANK() OVER (ORDER BY f.gross_margin_usd), 0)
      + 0.3 * coalesce(PERCENT_RANK() OVER (ORDER BY f.margin_pct NULLS FIRST), 0)
      - 0.1 * least(size(f.flags), 4) / 4.0
      - 0.1 * CASE WHEN f.has_open_ticket THEN 1 ELSE 0 END
    , 4)                                                                   AS health_score,
    SUM(f.gross_margin_usd) OVER (PARTITION BY f.country_code ORDER BY f.order_ts, f.order_id
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS country_cum_margin,
    AVG(f.margin_pct) OVER (PARTITION BY f.tier_at_order)                  AS tier_avg_margin_pct,
    f.gross_margin_usd - LAG(f.gross_margin_usd) OVER (PARTITION BY f.customer_id ORDER BY f.order_ts) AS margin_vs_prev_order
  FROM flagged f
)
-- 17. Final
SELECT /*+ REPARTITION(2) */
  RANK() OVER (ORDER BY health_score DESC, order_id)                       AS health_rank,
  order_id,
  customer_id,
  country_code,
  tier_at_order,
  status,
  channel,
  date_format(order_ts, 'yyyy-MM-dd')                                      AS order_date,
  main_cat_path,
  divisions,
  units,
  round(rev_usd, 2)                                                        AS rev_usd,
  round(cost_usd, 2)                                                       AS cost_usd,
  gross_margin_usd,
  margin_pct,
  round(discount_usd, 2)                                                   AS discount_usd,
  tickets,
  worst_priority,
  sla_breaches,
  avg_csat,
  flags,
  n_flags,
  health_score,
  CASE
    WHEN status = 'CANCELLED'                          THEN 'VOID'
    WHEN n_flags = 0 AND margin_pct >= 40              THEN 'HEALTHY'
    WHEN array_contains(flags, 'TICKET_RISK')
      OR array_contains(flags, 'PAY_MISMATCH')         THEN 'INVESTIGATE'
    WHEN margin_pct < 30                               THEN 'MARGIN_WATCH'
    ELSE 'OK'
  END                                                                      AS verdict,
  round(country_cum_margin, 2)                                             AS country_cum_margin,
  round(tier_avg_margin_pct, 2)                                            AS tier_avg_margin_pct,
  margin_vs_prev_order,
  NTILE(4) OVER (ORDER BY gross_margin_usd, order_id)                      AS margin_quartile,
  round(100 * gross_margin_usd / SUM(gross_margin_usd) OVER (), 2)        AS pct_of_total_margin
FROM scored
ORDER BY health_rank, order_id;
