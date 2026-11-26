-- Q12 part 2 of 2: THE BIG ONE: customer 360 scorecard
-- too long to convert in one go (3,992 tokens), so split by CTE; run parts 1-2 in order
-- needs temp views from earlier parts: c_clean, p_dim, lines, o_enriched, fx_asof, o_seq, sess
-- returns the result of Q12
-- @id: Q12_part2
WITH
sess_agg AS (
  SELECT
    customer_id,
    COUNT(*)                                               AS sessions,
    count_if(converted)                                    AS converting_sessions,
    round(avg((unix_timestamp(s_end) - unix_timestamp(s_start)) / 60.0), 1) AS avg_session_min,
    mode(device_type)                                      AS main_device
  FROM sess
  GROUP BY customer_id
),
-- 11. Customer-level rollup of orders
cust_orders AS (
  SELECT
    customer_id,
    COUNT(*)                                                                AS orders_total,
    count_if(status IN ('DELIVERED','SHIPPED'))                             AS orders_kept,
    count_if(status IN ('CANCELLED','RETURNED'))                            AS orders_lost,
    round(SUM(net_usd) FILTER (WHERE status IN ('DELIVERED','SHIPPED')), 2) AS kept_usd,
    round(SUM(discount_usd), 2)                                             AS discount_usd,
    round(avg(gap_days), 1)                                                 AS avg_gap_days,
    max(order_ts)                                                           AS last_order_ts,
    min(order_ts)                                                           AS first_order_ts,
    flatten(collect_list(categories))                                       AS all_cats_raw,
    collect_list(named_struct('seq', order_seq, 'id', order_id, 'usd', net_usd, 'status', status)) AS history,
    count_if(recon_status <> 'MATCHED' AND status <> 'CANCELLED')           AS recon_issues,
    count_if(best_promo IS NOT NULL)                                        AS promo_orders,
    max_by(channel, net_usd)                                                AS top_channel,
    percentile_approx(minutes_to_pay, 0.5)                                  AS median_minutes_to_pay
  FROM o_seq
  GROUP BY customer_id
),
-- 12. Category affinity: share of kept spend by category
cat_spend AS (
  SELECT oe.customer_id, p.category,
         SUM(l.net_local * fx.rate_to_usd) AS cat_usd
  FROM o_enriched oe
  JOIN lines l      ON l.order_id = oe.order_id
  JOIN p_dim p      ON p.product_id = l.product_id
  JOIN fx_asof fx   ON fx.order_id = oe.order_id
  WHERE oe.status IN ('DELIVERED','SHIPPED')
  GROUP BY oe.customer_id, p.category
),
cat_affinity AS (
  SELECT
    customer_id,
    map_from_entries(collect_list(struct(category, round(cat_usd / tot, 3)))) AS cat_share,
    max_by(category, cat_usd)                                                 AS fav_category
  FROM (
    SELECT *, SUM(cat_usd) OVER (PARTITION BY customer_id) AS tot FROM cat_spend
  )
  GROUP BY customer_id
),
-- 13. Assemble the 360 view
c360 AS (
  SELECT
    c.customer_id, c.full_name, c.email, c.email_domain,
    c.country_code,
    (c.email_cc_hint IS NOT NULL AND c.email_cc_hint <> c.country_code)   AS email_country_mismatch,
    c.tier, c.tier_rank, round(c.tenure_months, 1) AS tenure_months,
    coalesce(co.orders_total, 0)                                          AS orders_total,
    coalesce(co.orders_kept, 0)                                           AS orders_kept,
    coalesce(co.kept_usd, 0)                                              AS kept_usd,
    co.discount_usd,
    co.avg_gap_days,
    datediff(DATE'2024-08-01', to_date(co.last_order_ts))                 AS days_since_last,
    array_distinct(co.all_cats_raw)                                       AS bought_categories,
    array_except(c.interests, coalesce(co.all_cats_raw, array()))         AS untapped_interests,
    size(array_intersect(c.interests, coalesce(co.all_cats_raw, array()))) AS interest_hits,
    ca.cat_share,
    ca.fav_category,
    co.top_channel,
    co.promo_orders,
    co.recon_issues,
    co.median_minutes_to_pay,
    transform(
      array_sort(co.history, (a, b) -> a.seq - b.seq),
      h -> concat('#', h.seq, ':', h.status)
    )                                                                     AS order_timeline,
    aggregate(
      filter(co.history, h -> h.status = 'RETURNED'),
      CAST(0 AS DECIMAL(18,2)),
      -- accumulator type must stay DECIMAL(18,2); acc + usd widens to (19,2) without the cast
      (acc, h) -> CAST(acc + h.usd AS DECIMAL(18,2))
    )                                                                     AS returned_usd,
    coalesce(sa.sessions, 0)                                              AS sessions,
    sa.converting_sessions,
    sa.avg_session_min,
    sa.main_device,
    size(map_filter(c.preferences, (k, v) -> v = 'Y')) > 0                AS contactable
  FROM c_clean c
  LEFT JOIN cust_orders  co ON co.customer_id = c.customer_id
  LEFT JOIN cat_affinity ca ON ca.customer_id = c.customer_id
  LEFT JOIN sess_agg     sa ON sa.customer_id = c.customer_id
),
-- 14. Scoring and ranking
scored AS (
  SELECT
    *,
    round(
        0.40 * coalesce(PERCENT_RANK() OVER (ORDER BY kept_usd), 0)
      + 0.25 * coalesce(PERCENT_RANK() OVER (ORDER BY orders_kept), 0)
      + 0.20 * (1 - coalesce(PERCENT_RANK() OVER (ORDER BY days_since_last NULLS LAST), 1))
      + 0.15 * (tier_rank / 4.0)
      - 0.10 * least(recon_issues, 3) / 3.0
    , 4)                                                                    AS value_score
  FROM c360
)
SELECT
  DENSE_RANK() OVER (ORDER BY value_score DESC)                             AS overall_rank,
  RANK()       OVER (PARTITION BY country_code ORDER BY value_score DESC)   AS country_rank,
  customer_id,
  full_name,
  country_code,
  tier,
  value_score,
  CASE
    WHEN orders_total = 0                                   THEN 'PROSPECT'
    WHEN value_score >= 0.70                                THEN 'VIP'
    WHEN value_score >= 0.45 AND days_since_last <= 45      THEN 'ACTIVE'
    WHEN days_since_last > 90                               THEN 'LAPSING'
    ELSE 'DEVELOPING'
  END                                                                       AS lifecycle,
  CASE
    WHEN NOT contactable                         THEN 'none (no opt-in)'
    WHEN size(untapped_interests) > 0            THEN concat('cross-sell: ', array_join(untapped_interests, ', '))
    WHEN fav_category IS NOT NULL                THEN concat('replenish: ', fav_category)
    ELSE 'welcome series'
  END                                                                       AS next_best_action,
  orders_total, orders_kept, kept_usd, returned_usd, discount_usd,
  days_since_last, avg_gap_days,
  bought_categories, untapped_interests, fav_category,
  to_json(cat_share)                                                        AS cat_share_json,
  order_timeline,
  sessions, converting_sessions, avg_session_min, main_device,
  email_domain, email_country_mismatch,
  recon_issues, promo_orders, top_channel, median_minutes_to_pay,
  round(kept_usd - AVG(kept_usd) OVER (PARTITION BY country_code), 2)       AS kept_vs_country_avg,
  round(100 * kept_usd / nullif(SUM(kept_usd) OVER (), 0), 2)               AS pct_of_total_kept,
  NTH_VALUE(full_name, 2) OVER (
    PARTITION BY country_code ORDER BY value_score DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  )                                                                         AS country_runner_up
FROM scored
ORDER BY overall_rank, customer_id;
