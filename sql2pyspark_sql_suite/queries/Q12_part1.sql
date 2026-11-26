-- Q12 part 1 of 2: THE BIG ONE: customer 360 scorecard
-- too long to convert in one go (3,992 tokens), so split by CTE; run parts 1-2 in order
-- needs setup views: customers, products, orders, order_items, payments, fx_rates, web_events, promotions
-- creates temp views: c_clean, p_dim, lines, o_enriched, fx_asof, pay, promo_hits, o_fact, o_seq, sess
-- @id: Q12_part1
-- 1. Clean customers: normalize email/name, derive tenure
CREATE OR REPLACE TEMP VIEW c_clean AS
  SELECT
    customer_id,
    trim(regexp_replace(full_name, '\\s+', ' '))                           AS full_name,
    lower(nullif(trim(email), ''))                                          AS email,
    regexp_extract(lower(email), '@([a-z0-9.-]+)$', 1)                      AS email_domain,
    CASE WHEN regexp_extract(lower(email), '\\.([a-z]{2})$', 1) <> ''
         THEN upper(regexp_extract(lower(email), '\\.([a-z]{2})$', 1)) END  AS email_cc_hint,
    country_code,
    city,
    signup_date,
    coalesce(tier, 'BRONZE')                                                AS tier,
    CASE coalesce(tier, 'BRONZE')
      WHEN 'PLATINUM' THEN 4 WHEN 'GOLD' THEN 3 WHEN 'SILVER' THEN 2 ELSE 1
    END                                                                     AS tier_rank,
    months_between(DATE'2024-08-01', signup_date)                           AS tenure_months,
    interests,
    preferences
  FROM customers;

-- 2. Product dimension with parsed JSON attributes
CREATE OR REPLACE TEMP VIEW p_dim AS
  SELECT
    product_id, sku, product_name, category,
    coalesce(sub_category, 'n/a')                                     AS sub_category,
    coalesce(brand, 'Unbranded')                                      AS brand,
    list_price,
    CAST(get_json_object(attributes_json, '$.warranty.months') AS INT) AS warranty_months,
    from_json(get_json_object(attributes_json, '$.tags'), 'ARRAY<STRING>') AS tags
  FROM products;

-- 3. Line-level facts with gross / discount / net in local currency
CREATE OR REPLACE TEMP VIEW lines AS
  SELECT
    oi.order_id, oi.line_no, oi.product_id, oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price                                       AS gross_local,
    oi.quantity * oi.unit_price * oi.discount_pct                     AS discount_local,
    oi.quantity * oi.unit_price * (1 - oi.discount_pct)               AS net_local
  FROM order_items oi;

-- 4. Orders enriched with line aggregates and categories
CREATE OR REPLACE TEMP VIEW o_enriched AS
  SELECT
    o.order_id, o.customer_id, o.order_ts, to_date(o.order_ts) AS order_date,
    o.status, o.channel, o.currency,
    o.ship_to.city                                                    AS ship_city,
    COUNT(*)                                                          AS n_lines,
    SUM(l.quantity)                                                   AS units,
    SUM(l.gross_local)                                                AS gross_local,
    SUM(l.discount_local)                                             AS discount_local,
    SUM(l.net_local)                                                  AS net_local,
    array_sort(collect_set(p.category))                               AS categories,
    max_by(p.product_name, l.net_local)                               AS top_product,
    min_by(p.product_name, l.net_local)                               AS bottom_product,
    max(p.warranty_months)                                            AS max_warranty_months
  FROM orders o
  JOIN lines l  ON l.order_id = o.order_id
  JOIN p_dim p  ON p.product_id = l.product_id
  GROUP BY o.order_id, o.customer_id, o.order_ts, o.status, o.channel, o.currency, o.ship_to.city;

-- 5. As-of FX rates
CREATE OR REPLACE TEMP VIEW fx_asof AS
  SELECT order_id, rate_to_usd FROM (
    SELECT /*+ BROADCAST(f) */
      oe.order_id, f.rate_to_usd,
      ROW_NUMBER() OVER (PARTITION BY oe.order_id ORDER BY f.rate_date DESC) AS rn
    FROM o_enriched oe
    JOIN fx_rates f ON f.currency = oe.currency AND f.rate_date <= oe.order_date
  ) WHERE rn = 1;

-- 6. Payments reconciliation per order
CREATE OR REPLACE TEMP VIEW pay AS
  SELECT
    order_id,
    SUM(CASE WHEN payment_status = 'CAPTURED' THEN amount ELSE 0 END)  AS captured_local,
    SUM(CASE WHEN payment_status = 'REFUNDED' THEN amount ELSE 0 END)  AS refunded_local,
    COUNT(DISTINCT method)                                             AS n_methods,
    concat_ws('+', array_sort(collect_set(method)))                    AS methods,
    max(paid_ts)                                                       AS last_paid_ts,
    bool_or(payment_status = 'FAILED')                                 AS any_failed
  FROM payments
  GROUP BY order_id;

-- 7. Promotions applicable to each order (date range + array overlap)
CREATE OR REPLACE TEMP VIEW promo_hits AS
  SELECT
    oe.order_id,
    max_by(pr.promo_code, pr.promo_pct)                                AS best_promo,
    max(pr.promo_pct)                                                  AS best_promo_pct
  FROM o_enriched oe
  JOIN promotions pr
    ON oe.order_date BETWEEN pr.start_date AND pr.end_date
   AND (size(pr.eligible_categories) = 0 OR arrays_overlap(pr.eligible_categories, oe.categories))
  GROUP BY oe.order_id;

-- 8. Order fact in USD with reconciliation flags
CREATE OR REPLACE TEMP VIEW o_fact AS
  SELECT
    oe.*,
    fx.rate_to_usd,
    CAST(oe.net_local * fx.rate_to_usd AS DECIMAL(18,2))                AS net_usd,
    CAST(oe.discount_local * fx.rate_to_usd AS DECIMAL(18,2))           AS discount_usd,
    coalesce(pay.captured_local, 0)                                     AS captured_local,
    coalesce(pay.refunded_local, 0)                                     AS refunded_local,
    pay.methods,
    coalesce(pay.any_failed, false)                                     AS any_failed_payment,
    CASE
      WHEN pay.order_id IS NULL                                          THEN 'NO_PAYMENT'
      WHEN abs(coalesce(pay.captured_local, 0) - oe.net_local) <= 1       THEN 'MATCHED'
      WHEN coalesce(pay.captured_local, 0) > oe.net_local                 THEN 'OVERPAID'
      WHEN coalesce(pay.captured_local, 0) = 0                            THEN 'UNPAID'
      ELSE 'UNDERPAID'
    END                                                                 AS recon_status,
    ph.best_promo,
    ph.best_promo_pct,
    (unix_timestamp(pay.last_paid_ts) - unix_timestamp(oe.order_ts)) / 60.0 AS minutes_to_pay
  FROM o_enriched oe
  LEFT JOIN fx_asof    fx  ON fx.order_id  = oe.order_id
  LEFT JOIN pay              ON pay.order_id = oe.order_id
  LEFT JOIN promo_hits ph  ON ph.order_id  = oe.order_id;

-- 9. Order sequencing per customer
CREATE OR REPLACE TEMP VIEW o_seq AS
  SELECT
    f.*,
    ROW_NUMBER() OVER w_cust                                            AS order_seq,
    COUNT(*)     OVER (PARTITION BY customer_id)                        AS cust_order_cnt,
    datediff(order_date, LAG(order_date) OVER w_cust)                   AS gap_days,
    SUM(CASE WHEN status IN ('DELIVERED','SHIPPED') THEN net_usd ELSE 0 END)
      OVER (PARTITION BY customer_id ORDER BY order_ts, order_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS cum_kept_usd
  FROM o_fact f
  WINDOW w_cust AS (PARTITION BY customer_id ORDER BY order_ts, order_id);

-- 10. Sessions (re-uses Q01 logic, compressed)
CREATE OR REPLACE TEMP VIEW sess AS
  SELECT customer_id, session_seq,
         MIN(event_ts) AS s_start, MAX(event_ts) AS s_end,
         max(event_type = 'checkout') AS converted,
         max(get_json_object(payload, '$.device.type')) AS device_type
  FROM (
    SELECT *, SUM(new_s) OVER (PARTITION BY customer_id ORDER BY event_ts, event_id) AS session_seq
    FROM (
      SELECT *,
        CASE WHEN event_ts > LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts, event_id)
                             + INTERVAL 30 MINUTES
               OR LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts, event_id) IS NULL
             THEN 1 ELSE 0 END AS new_s
      FROM web_events
      WHERE customer_id IS NOT NULL
    )
  )
  GROUP BY customer_id, session_seq;
