-- Q15b: Tier timeline per customer: days per tier, upgrades/downgrades, drift check
-- needs setup views: customers, tier_history
-- @id: Q15b
WITH th AS (
  SELECT *, LAG(tier) OVER (PARTITION BY customer_id ORDER BY effective_from) AS prev_t,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY effective_from) AS rn
  FROM tier_history
),
scd AS (
  SELECT
    customer_id, tier,
    effective_from AS valid_from,
    LEAD(effective_from, 1, DATE'9999-12-31') OVER (PARTITION BY customer_id ORDER BY effective_from) AS valid_to,
    LAG(tier) OVER (PARTITION BY customer_id ORDER BY effective_from) AS prev_tier
  FROM th
  WHERE rn = 1 OR NOT (tier <=> prev_t)
),
ranked AS (
  SELECT s.*,
         CASE s.tier WHEN 'BRONZE' THEN 1 WHEN 'SILVER' THEN 2 WHEN 'GOLD' THEN 3 WHEN 'PLATINUM' THEN 4 END AS r,
         CASE s.prev_tier WHEN 'BRONZE' THEN 1 WHEN 'SILVER' THEN 2 WHEN 'GOLD' THEN 3 WHEN 'PLATINUM' THEN 4 END AS pr,
         datediff(least(s.valid_to, DATE'2024-08-01'), s.valid_from) AS days_in_version
  FROM scd s
),
tier_days AS (
  -- aggregate first: a tier can recur (7: SILVER twice) and map keys must be unique
  SELECT customer_id, coalesce(tier, 'NONE') AS tier_key, SUM(days_in_version) AS days
  FROM ranked
  GROUP BY customer_id, coalesce(tier, 'NONE')
),
cust_hist AS (
SELECT
  c.customer_id,
  c.full_name,
  c.tier                                                                     AS tier_on_profile,
  max_by(r.tier, r.valid_from)                                               AS tier_from_history,
  NOT (c.tier <=> max_by(r.tier, r.valid_from))                              AS profile_drift,
  COUNT(r.valid_from)                                                        AS versions,
  count_if(r.r > r.pr)                                                       AS upgrades,
  count_if(r.r < r.pr)                                                       AS downgrades,
  MAX(r.r)                                                                   AS peak_rank,
  concat_ws(' -> ', transform(array_sort(collect_list(struct(r.valid_from, r.tier))),
                              x -> coalesce(x.tier, 'NONE')))                AS timeline,
  MIN(r.valid_from)                                                          AS first_seen
FROM customers c
LEFT JOIN ranked r ON r.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name, c.tier
)
SELECT
  h.*,
  -- correlated scalar subquery returning a MAP (not allowed inside the grouped SELECT above)
  (SELECT map_from_entries(array_sort(collect_list(struct(td.tier_key, td.days))))
     FROM tier_days td WHERE td.customer_id = h.customer_id)                 AS days_per_tier
FROM cust_hist h
ORDER BY h.customer_id;
