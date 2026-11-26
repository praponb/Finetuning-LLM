-- Q02: RFM segmentation with NTILE, PERCENT_RANK and percentile_approx
-- needs setup views: customers, orders, order_items, fx_rates
-- @id: Q02
WITH line_values AS (
  SELECT
    o.customer_id,
    o.order_id,
    o.order_ts,
    oi.quantity * oi.unit_price * (1 - oi.discount_pct) AS line_net_local,
    o.currency
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  WHERE o.status NOT IN ('CANCELLED', 'PENDING')
),
fx_latest AS (
  SELECT currency, rate_to_usd
  FROM (
    SELECT currency, rate_to_usd,
           ROW_NUMBER() OVER (PARTITION BY currency ORDER BY rate_date DESC) AS rn
    FROM fx_rates
  ) t
  WHERE rn = 1
),
cust AS (
  SELECT
    lv.customer_id,
    datediff(DATE'2024-08-01', to_date(MAX(lv.order_ts)))   AS recency_days,
    COUNT(DISTINCT lv.order_id)                             AS frequency,
    round(SUM(lv.line_net_local * fx.rate_to_usd), 2)       AS monetary_usd
  FROM line_values lv
  LEFT JOIN fx_latest fx ON fx.currency = lv.currency
  GROUP BY lv.customer_id
),
scored AS (
  SELECT
    c.*,
    6 - NTILE(5) OVER (ORDER BY recency_days ASC)        AS r_score,
    NTILE(5) OVER (ORDER BY frequency ASC, customer_id)  AS f_score,
    NTILE(5) OVER (ORDER BY monetary_usd ASC)            AS m_score,
    round(PERCENT_RANK() OVER (ORDER BY monetary_usd), 3) AS monetary_pct_rank,
    CUME_DIST() OVER (ORDER BY monetary_usd)             AS monetary_cume_dist
  FROM cust c
),
thresholds AS (
  SELECT
    percentile_approx(monetary_usd, 0.5)               AS p50_usd,
    percentile_approx(monetary_usd, array(0.25, 0.75)) AS iqr_usd,
    avg(monetary_usd)                                  AS avg_usd,
    stddev_samp(monetary_usd)                          AS sd_usd
  FROM scored
)
SELECT
  cu.customer_id,
  cu.full_name,
  upper(cu.country_code)                                         AS country,
  coalesce(cu.tier, 'UNASSIGNED')                                AS tier,
  s.recency_days, s.frequency, s.monetary_usd,
  concat(s.r_score, s.f_score, s.m_score)                        AS rfm_code,
  s.r_score + s.f_score + s.m_score                              AS rfm_total,
  CASE
    WHEN s.r_score >= 4 AND s.f_score >= 4 AND s.m_score >= 4 THEN 'Champion'
    WHEN s.r_score >= 3 AND s.f_score >= 3                    THEN 'Loyal'
    WHEN s.r_score >= 4 AND s.f_score <= 2                    THEN 'New / Promising'
    WHEN s.r_score <= 2 AND s.m_score >= 4                    THEN 'At-risk whale'
    WHEN s.r_score <= 2                                       THEN 'Hibernating'
    ELSE 'Needs attention'
  END                                                            AS segment,
  s.monetary_usd > t.p50_usd                                     AS above_median,
  round(try_divide(s.monetary_usd - t.avg_usd, t.sd_usd), 3)     AS monetary_zscore,
  s.monetary_usd NOT BETWEEN t.iqr_usd[0] AND t.iqr_usd[1]       AS outside_iqr,
  s.monetary_pct_rank
FROM scored s
JOIN customers cu ON cu.customer_id = s.customer_id
CROSS JOIN thresholds t
ORDER BY rfm_total DESC, s.monetary_usd DESC;
