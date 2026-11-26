-- Q15a: Tier in force at the time of each order
-- needs setup views: customers, orders, order_items, tier_history
-- @id: Q15a
WITH th AS (
  SELECT
    *,
    LAG(tier) OVER (PARTITION BY customer_id ORDER BY effective_from) AS prev_tier_raw,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY effective_from) AS rn
  FROM tier_history
),
changes AS (
  -- null-safe: NULL -> NULL is not a change, NULL -> 'BRONZE' is
  SELECT customer_id, tier, effective_from, change_source
  FROM th
  WHERE rn = 1 OR tier IS DISTINCT FROM prev_tier_raw
),
scd AS (
  SELECT
    customer_id,
    tier,
    LAG(tier) OVER (PARTITION BY customer_id ORDER BY effective_from)                       AS prev_tier,
    effective_from                                                                           AS valid_from,
    LEAD(effective_from, 1, DATE'9999-12-31') OVER (PARTITION BY customer_id ORDER BY effective_from) AS valid_to,
    change_source
  FROM changes
),
tier_rank AS (
  SELECT * FROM VALUES ('BRONZE', 1, 0.00), ('SILVER', 2, 0.02), ('GOLD', 3, 0.05), ('PLATINUM', 4, 0.08)
  AS t(tier, tier_rank, member_discount)
),
order_net AS (
  SELECT o.order_id, o.customer_id, o.order_ts, to_date(o.order_ts) AS order_date, o.status,
         SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct)) AS net_local,
         MAX(oi.discount_pct)                                      AS max_line_discount
  FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
  GROUP BY o.order_id, o.customer_id, o.order_ts, o.status
)
SELECT
  o.order_id,
  o.customer_id,
  o.order_ts,
  coalesce(s.tier, 'UNKNOWN')                                              AS tier_at_order,
  c.tier                                                                   AS tier_today,
  s.tier <=> c.tier                                                        AS same_tier_as_today,
  s.valid_from                                                             AS tier_since,
  CASE WHEN s.valid_to = DATE'9999-12-31' THEN NULL ELSE s.valid_to END    AS tier_until,
  datediff(o.order_date, s.valid_from)                                     AS days_in_tier,
  tr.tier_rank,
  tr.member_discount,
  o.max_line_discount > coalesce(tr.member_discount, 0)                    AS discount_above_entitlement,
  (SELECT COUNT(*) FROM scd s2
    WHERE s2.customer_id = o.customer_id AND s2.valid_from <= o.order_date) AS tier_versions_so_far,
  CASE
    WHEN s.prev_tier IS NULL AND s.tier IS NULL THEN 'none'
    WHEN s.prev_tier IS NULL                    THEN 'initial'
    WHEN ptr.tier_rank < tr.tier_rank           THEN 'upgraded'
    WHEN ptr.tier_rank > tr.tier_rank           THEN 'downgraded'
    ELSE 'from-null'
  END                                                                      AS last_tier_move,
  round(o.net_local, 2)                                                    AS net_local
FROM order_net o
LEFT JOIN scd s
  ON  s.customer_id = o.customer_id
  AND o.order_date >= s.valid_from
  AND o.order_date <  s.valid_to
LEFT JOIN tier_rank tr  ON tr.tier  = s.tier
LEFT JOIN tier_rank ptr ON ptr.tier = s.prev_tier
JOIN customers c ON c.customer_id = o.customer_id
ORDER BY o.order_id;
