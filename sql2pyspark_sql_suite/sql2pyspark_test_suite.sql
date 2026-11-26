-- =====================================================================================
--  sql2pyspark — Spark SQL conversion test suite
--  ---------------------------------------------------------------------------------
--  Self-contained: Section 0 builds all source data as TEMP VIEWs, so every query
--  runs on a bare SparkSession with no external tables.
--
--  Each statement is preceded by "-- @id: <ID>" so compare_harness.py can pair it
--  with conversions/<ID>.py. IDs starting with "setup:" build the source data.
--
--  Target: Spark 3.4+ (Q10 uses UNPIVOT, 3.4+). Verified on Spark 4.x (ANSI mode on).
--
--  Feature coverage (for checking the converter):
--    Q01  Sessionization: LAG, gap flags, running SUM window, from_json, get_json_object
--    Q02  RFM scoring: NTILE, PERCENT_RANK, percentile_approx, CASE ladders, datediff
--    Q03  PIVOT with multiple aggregates + IN alias list
--    Q04  GROUPING SETS / ROLLUP / CUBE, grouping(), grouping_id(), FILTER (WHERE ...)
--    Q05  Higher-order functions: transform, filter, aggregate, exists, forall,
--         array_sort w/ comparator, map_filter, transform_values, zip_with,
--         LATERAL VIEW [OUTER] explode / posexplode, inline
--    Q06  LEFT SEMI / LEFT ANTI, correlated EXISTS / NOT EXISTS, IN subquery,
--         scalar correlated subquery in SELECT
--    Q07  UNION ALL / UNION / INTERSECT / EXCEPT, set ops inside CTEs
--    Q08  Point-in-time FX join via ROW_NUMBER, RANGE BETWEEN INTERVAL frames,
--         FIRST_VALUE/LAST_VALUE IGNORE NULLS, max_by/min_by, broadcast hint
--    Q09  Funnel analysis: conditional aggregation, MIN(CASE), timestamp diffs,
--         array_contains, collect_list + array_distinct + concat_ws
--    Q10  UNPIVOT + stack() equivalent
--    Q11  Market-basket self-join with anti-duplication, CROSS JOIN, FULL OUTER JOIN
--    Q12  The "everything" query: 14 CTEs chained, all of the above combined
-- =====================================================================================


-- =====================================================================================
-- SECTION 0 — SOURCE DATA
-- =====================================================================================

-- @id: setup:customers
CREATE OR REPLACE TEMP VIEW customers AS
SELECT * FROM VALUES
  (1,  'Alice Tan',       'alice.tan@example.com',   'TH', 'Bangkok',     DATE'2022-01-15', 'GOLD',     map('newsletter','Y','sms','N'),  array('electronics','books')),
  (2,  'Bob Srisai',      'bob.s@example.co.th',     'TH', 'Chiang Mai',  DATE'2022-03-02', 'SILVER',   map('newsletter','N','sms','Y'),  array('fashion')),
  (3,  'Chen Wei',        'chen.wei@example.sg',     'SG', 'Singapore',   DATE'2021-11-20', 'PLATINUM', map('newsletter','Y','sms','Y'),  array('electronics','home','beauty')),
  (4,  'Dewi Lestari',    'dewi@example.id',         'ID', 'Jakarta',     DATE'2023-06-10', 'BRONZE',   map('newsletter','Y'),            array()),
  (5,  'Eko Prasetyo',    NULL,                      'ID', 'Surabaya',    DATE'2023-07-01', NULL,       map(),                            array('home')),
  (6,  'Fah Wongsa',      'fah.w@example.com',       'TH', 'Bangkok',     DATE'2024-02-29', 'SILVER',   map('sms','Y'),                   array('beauty','fashion')),
  (7,  'Gina Reyes',      'GINA.REYES@EXAMPLE.PH',   'PH', 'Manila',      DATE'2022-08-08', 'GOLD',     map('newsletter','N'),            array('books','home')),
  (8,  'Hoang Nguyen',    'hoang.n@example.vn',      'VN', 'Hanoi',       DATE'2024-05-05', 'BRONZE',   map('newsletter','Y','sms','N'),  array('electronics')),
  (9,  'Ivy Lim',         'ivy.lim@example.my',      'MY', 'Kuala Lumpur',DATE'2021-01-01', 'PLATINUM', map('newsletter','Y','sms','Y'),  array('fashion','beauty','home')),
  (10, 'Jon Inactive',    'jon@example.com',         'TH', 'Phuket',      DATE'2020-12-12', 'BRONZE',   map(),                            array())
AS customers(customer_id, full_name, email, country_code, city, signup_date, tier, preferences, interests);

-- @id: setup:products
CREATE OR REPLACE TEMP VIEW products AS
SELECT * FROM VALUES
  (101, 'SKU-EL-0001', 'Wireless Earbuds Pro',      'electronics', 'audio',      'Sonix',   CAST(129.90 AS DECIMAL(10,2)), '{"color":"black","weight_g":55,"tags":["bluetooth","anc"],"warranty":{"months":12}}'),
  (102, 'SKU-EL-0002', '4K Action Camera',          'electronics', 'camera',     'Vista',   CAST(349.00 AS DECIMAL(10,2)), '{"color":"grey","weight_g":120,"tags":["4k","waterproof"],"warranty":{"months":24}}'),
  (103, 'SKU-EL-0003', 'USB-C Charger 65W',         'electronics', 'accessory',  'Voltix',  CAST(39.50  AS DECIMAL(10,2)), '{"color":"white","weight_g":110,"tags":["gan"],"warranty":{"months":6}}'),
  (201, 'SKU-FA-0001', 'Linen Shirt',               'fashion',     'tops',       'Kora',    CAST(45.00  AS DECIMAL(10,2)), '{"color":"beige","sizes":["S","M","L"],"tags":["summer"]}'),
  (202, 'SKU-FA-0002', 'Running Shoes',             'fashion',     'footwear',   'Strida',  CAST(89.99  AS DECIMAL(10,2)), '{"color":"blue","sizes":["40","41","42","43"],"tags":["sport","mesh"]}'),
  (301, 'SKU-HO-0001', 'Ceramic Pour-Over Set',     'home',        'kitchen',    'Mori',    CAST(32.00  AS DECIMAL(10,2)), '{"color":"white","tags":["coffee"]}'),
  (302, 'SKU-HO-0002', 'Air Purifier Mini',         'home',        'appliance',  'Breeze',  CAST(159.00 AS DECIMAL(10,2)), '{"color":"white","weight_g":2100,"tags":["hepa"],"warranty":{"months":24}}'),
  (401, 'SKU-BE-0001', 'Vitamin C Serum',           'beauty',      'skincare',   'Glowa',   CAST(24.90  AS DECIMAL(10,2)), '{"volume_ml":30,"tags":["vegan"]}'),
  (402, 'SKU-BE-0002', 'Mineral Sunscreen SPF50',   'beauty',      'suncare',    'Glowa',   CAST(19.90  AS DECIMAL(10,2)), '{"volume_ml":50,"tags":["reef-safe","vegan"]}'),
  (501, 'SKU-BO-0001', 'Data Engineering Handbook', 'books',       'tech',       'Readly',  CAST(55.00  AS DECIMAL(10,2)), '{"pages":640,"tags":["paperback"]}'),
  (999, 'SKU-XX-9999', 'Discontinued Widget',       'misc',        NULL,         NULL,      CAST(1.00   AS DECIMAL(10,2)), NULL)
AS products(product_id, sku, product_name, category, sub_category, brand, list_price, attributes_json);

-- @id: setup:orders
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1001, 1, TIMESTAMP'2024-01-03 10:15:00', 'DELIVERED',  'web',    'THB', named_struct('street','12 Sukhumvit 21','city','Bangkok','zip','10110')),
  (1002, 1, TIMESTAMP'2024-02-14 20:41:00', 'DELIVERED',  'app',    'THB', named_struct('street','12 Sukhumvit 21','city','Bangkok','zip','10110')),
  (1003, 2, TIMESTAMP'2024-02-15 08:05:00', 'CANCELLED',  'web',    'THB', named_struct('street','5 Nimman Rd','city','Chiang Mai','zip','50200')),
  (1004, 3, TIMESTAMP'2024-03-01 12:00:00', 'DELIVERED',  'app',    'SGD', named_struct('street','1 Orchard Rd','city','Singapore','zip','238801')),
  (1005, 3, TIMESTAMP'2024-03-20 23:59:59', 'RETURNED',   'app',    'SGD', named_struct('street','1 Orchard Rd','city','Singapore','zip','238801')),
  (1006, 4, TIMESTAMP'2024-04-02 09:30:00', 'DELIVERED',  'market', 'IDR', named_struct('street','Jl. Sudirman 8','city','Jakarta','zip','10220')),
  (1007, 5, TIMESTAMP'2024-04-18 14:22:00', 'SHIPPED',    'web',    'IDR', NULL),
  (1008, 6, TIMESTAMP'2024-05-05 19:00:00', 'DELIVERED',  'app',    'THB', named_struct('street','88 Silom','city','Bangkok','zip','10500')),
  (1009, 7, TIMESTAMP'2024-05-30 07:45:00', 'DELIVERED',  'web',    'PHP', named_struct('street','23 Ayala Ave','city','Makati','zip','1226')),
  (1010, 1, TIMESTAMP'2024-06-11 11:11:00', 'DELIVERED',  'web',    'THB', named_struct('street','12 Sukhumvit 21','city','Bangkok','zip','10110')),
  (1011, 8, TIMESTAMP'2024-06-12 16:00:00', 'PENDING',    'app',    'VND', named_struct('street','15 Hang Bai','city','Hanoi','zip','100000')),
  (1012, 9, TIMESTAMP'2024-06-20 21:30:00', 'DELIVERED',  'market', 'MYR', named_struct('street','3 Jalan Ampang','city','Kuala Lumpur','zip','50450')),
  (1013, 9, TIMESTAMP'2024-07-01 10:00:00', 'DELIVERED',  'app',    'MYR', named_struct('street','3 Jalan Ampang','city','Kuala Lumpur','zip','50450')),
  (1014, 3, TIMESTAMP'2024-07-04 13:13:13', 'DELIVERED',  'web',    'SGD', named_struct('street','1 Orchard Rd','city','Singapore','zip','238801')),
  (1015, 6, TIMESTAMP'2024-07-15 18:20:00', 'SHIPPED',    'app',    'THB', named_struct('street','88 Silom','city','Bangkok','zip','10500')),
  (1016, 2, TIMESTAMP'2024-07-20 09:00:00', 'DELIVERED',  'web',    'THB', named_struct('street','5 Nimman Rd','city','Chiang Mai','zip','50200'))
AS orders(order_id, customer_id, order_ts, status, channel, currency, ship_to);

-- @id: setup:order_items
CREATE OR REPLACE TEMP VIEW order_items AS
SELECT * FROM VALUES
  (1001, 1, 101, 1, CAST(4500.00  AS DECIMAL(12,2)), 0.00),
  (1001, 2, 103, 2, CAST(1350.00  AS DECIMAL(12,2)), 0.10),
  (1002, 1, 401, 3, CAST(850.00   AS DECIMAL(12,2)), 0.00),
  (1002, 2, 501, 1, CAST(1900.00  AS DECIMAL(12,2)), 0.05),
  (1003, 1, 202, 1, CAST(3100.00  AS DECIMAL(12,2)), 0.00),
  (1004, 1, 102, 1, CAST(470.00   AS DECIMAL(12,2)), 0.15),
  (1004, 2, 101, 1, CAST(175.00   AS DECIMAL(12,2)), 0.00),
  (1004, 3, 103, 1, CAST(53.00    AS DECIMAL(12,2)), 0.00),
  (1005, 1, 302, 1, CAST(215.00   AS DECIMAL(12,2)), 0.00),
  (1006, 1, 301, 2, CAST(500000.00 AS DECIMAL(12,2)), 0.00),
  (1006, 2, 401, 1, CAST(390000.00 AS DECIMAL(12,2)), 0.20),
  (1007, 1, 302, 1, CAST(2500000.00 AS DECIMAL(12,2)), 0.00),
  (1008, 1, 401, 2, CAST(850.00   AS DECIMAL(12,2)), 0.00),
  (1008, 2, 201, 1, CAST(1550.00  AS DECIMAL(12,2)), 0.00),
  (1008, 3, 202, 1, CAST(3100.00  AS DECIMAL(12,2)), 0.10),
  (1009, 1, 501, 1, CAST(3100.00  AS DECIMAL(12,2)), 0.00),
  (1009, 2, 301, 1, CAST(1800.00  AS DECIMAL(12,2)), 0.00),
  (1010, 1, 103, 1, CAST(1350.00  AS DECIMAL(12,2)), 0.00),
  (1010, 2, 101, 1, CAST(4500.00  AS DECIMAL(12,2)), 0.12),
  (1011, 1, 101, 1, CAST(3200000.00 AS DECIMAL(12,2)), 0.00),
  (1012, 1, 201, 2, CAST(210.00   AS DECIMAL(12,2)), 0.00),
  (1012, 2, 401, 1, CAST(115.00   AS DECIMAL(12,2)), 0.00),
  (1013, 1, 302, 1, CAST(740.00   AS DECIMAL(12,2)), 0.05),
  (1014, 1, 501, 2, CAST(74.00    AS DECIMAL(12,2)), 0.00),
  (1014, 2, 999, 5, CAST(1.30     AS DECIMAL(12,2)), 0.00),
  (1015, 1, 202, 1, CAST(3100.00  AS DECIMAL(12,2)), 0.25),
  (1016, 1, 201, 1, CAST(1550.00  AS DECIMAL(12,2)), 0.00),
  (1016, 2, 103, 1, CAST(1350.00  AS DECIMAL(12,2)), 0.00)
AS order_items(order_id, line_no, product_id, quantity, unit_price, discount_pct);

-- @id: setup:payments
CREATE OR REPLACE TEMP VIEW payments AS
SELECT * FROM VALUES
  ('P-0001', 1001, 'card',     CAST(7130.00    AS DECIMAL(14,2)), TIMESTAMP'2024-01-03 10:16:00', 'CAPTURED'),
  ('P-0002', 1002, 'wallet',   CAST(4355.00    AS DECIMAL(14,2)), TIMESTAMP'2024-02-14 20:42:10', 'CAPTURED'),
  ('P-0003', 1003, 'card',     CAST(3100.00    AS DECIMAL(14,2)), TIMESTAMP'2024-02-15 08:06:00', 'REFUNDED'),
  ('P-0004', 1004, 'card',     CAST(300.00     AS DECIMAL(14,2)), TIMESTAMP'2024-03-01 12:01:00', 'CAPTURED'),
  ('P-0005', 1004, 'giftcard', CAST(327.50     AS DECIMAL(14,2)), TIMESTAMP'2024-03-01 12:01:30', 'CAPTURED'),
  ('P-0006', 1005, 'card',     CAST(215.00     AS DECIMAL(14,2)), TIMESTAMP'2024-03-21 00:00:20', 'REFUNDED'),
  ('P-0007', 1006, 'cod',      CAST(1312000.00 AS DECIMAL(14,2)), TIMESTAMP'2024-04-05 15:00:00', 'CAPTURED'),
  ('P-0008', 1007, 'transfer', CAST(2500000.00 AS DECIMAL(14,2)), TIMESTAMP'2024-04-18 14:30:00', 'PENDING'),
  ('P-0009', 1008, 'wallet',   CAST(6040.00    AS DECIMAL(14,2)), TIMESTAMP'2024-05-05 19:01:00', 'CAPTURED'),
  ('P-0010', 1009, 'card',     CAST(4900.00    AS DECIMAL(14,2)), TIMESTAMP'2024-05-30 07:46:00', 'CAPTURED'),
  ('P-0011', 1010, 'card',     CAST(5310.00    AS DECIMAL(14,2)), TIMESTAMP'2024-06-11 11:12:00', 'CAPTURED'),
  ('P-0012', 1011, 'wallet',   CAST(3200000.00 AS DECIMAL(14,2)), TIMESTAMP'2024-06-12 16:01:00', 'FAILED'),
  ('P-0013', 1012, 'card',     CAST(535.00     AS DECIMAL(14,2)), TIMESTAMP'2024-06-20 21:31:00', 'CAPTURED'),
  ('P-0014', 1013, 'card',     CAST(703.00     AS DECIMAL(14,2)), TIMESTAMP'2024-07-01 10:01:00', 'CAPTURED'),
  ('P-0015', 1014, 'card',     CAST(154.50     AS DECIMAL(14,2)), TIMESTAMP'2024-07-04 13:14:00', 'CAPTURED'),
  ('P-0016', 1015, 'wallet',   CAST(2325.00    AS DECIMAL(14,2)), TIMESTAMP'2024-07-15 18:21:00', 'CAPTURED')
AS payments(payment_id, order_id, method, amount, paid_ts, payment_status);

-- @id: setup:fx_rates
CREATE OR REPLACE TEMP VIEW fx_rates AS
SELECT * FROM VALUES
  ('THB', DATE'2024-01-01', 0.0285), ('THB', DATE'2024-04-01', 0.0272), ('THB', DATE'2024-07-01', 0.0275),
  ('SGD', DATE'2024-01-01', 0.7550), ('SGD', DATE'2024-04-01', 0.7410), ('SGD', DATE'2024-07-01', 0.7390),
  ('IDR', DATE'2024-01-01', 0.0000645), ('IDR', DATE'2024-04-01', 0.0000628),
  ('PHP', DATE'2024-01-01', 0.0180), ('PHP', DATE'2024-05-01', 0.0174),
  ('MYR', DATE'2024-01-01', 0.2150), ('MYR', DATE'2024-06-01', 0.2125),
  ('VND', DATE'2024-01-01', 0.0000410)
AS fx_rates(currency, rate_date, rate_to_usd);

-- @id: setup:web_events
CREATE OR REPLACE TEMP VIEW web_events AS
SELECT * FROM VALUES
  (1,  1, TIMESTAMP'2024-01-03 09:50:00', 'page_view',   '{"page":"/home","referrer":"google","device":{"os":"iOS","type":"mobile"}}'),
  (2,  1, TIMESTAMP'2024-01-03 09:55:00', 'search',      '{"page":"/search","query":"earbuds","results":24,"device":{"os":"iOS","type":"mobile"}}'),
  (3,  1, TIMESTAMP'2024-01-03 10:02:00', 'product_view','{"page":"/p/101","product_id":101,"device":{"os":"iOS","type":"mobile"}}'),
  (4,  1, TIMESTAMP'2024-01-03 10:10:00', 'add_to_cart', '{"page":"/p/101","product_id":101,"qty":1,"device":{"os":"iOS","type":"mobile"}}'),
  (5,  1, TIMESTAMP'2024-01-03 10:14:30', 'checkout',    '{"page":"/checkout","cart_value":7130,"device":{"os":"iOS","type":"mobile"}}'),
  (6,  1, TIMESTAMP'2024-01-03 15:00:00', 'page_view',   '{"page":"/home","referrer":"direct","device":{"os":"macOS","type":"desktop"}}'),
  (7,  2, TIMESTAMP'2024-02-15 07:40:00', 'page_view',   '{"page":"/home","referrer":"facebook","device":{"os":"Android","type":"mobile"}}'),
  (8,  2, TIMESTAMP'2024-02-15 07:58:00', 'product_view','{"page":"/p/202","product_id":202,"device":{"os":"Android","type":"mobile"}}'),
  (9,  2, TIMESTAMP'2024-02-15 08:01:00', 'add_to_cart', '{"page":"/p/202","product_id":202,"qty":1,"device":{"os":"Android","type":"mobile"}}'),
  (10, 2, TIMESTAMP'2024-02-15 08:04:00', 'checkout',    '{"page":"/checkout","cart_value":3100,"device":{"os":"Android","type":"mobile"}}'),
  (11, 3, TIMESTAMP'2024-03-01 11:20:00', 'search',      '{"page":"/search","query":"action camera 4k","results":7,"device":{"os":"Windows","type":"desktop"}}'),
  (12, 3, TIMESTAMP'2024-03-01 11:25:00', 'product_view','{"page":"/p/102","product_id":102,"device":{"os":"Windows","type":"desktop"}}'),
  (13, 3, TIMESTAMP'2024-03-01 11:40:00', 'add_to_cart', '{"page":"/p/102","product_id":102,"qty":1,"device":{"os":"Windows","type":"desktop"}}'),
  (14, 3, TIMESTAMP'2024-03-01 11:59:00', 'checkout',    '{"page":"/checkout","cart_value":627.5,"device":{"os":"Windows","type":"desktop"}}'),
  (15, 4, TIMESTAMP'2024-04-01 22:00:00', 'page_view',   '{"page":"/home","referrer":"tiktok","device":{"os":"Android","type":"mobile"}}'),
  (16, 4, TIMESTAMP'2024-04-02 09:10:00', 'product_view','{"page":"/p/301","product_id":301,"device":{"os":"Android","type":"mobile"}}'),
  (17, 4, TIMESTAMP'2024-04-02 09:20:00', 'add_to_cart', '{"page":"/p/301","product_id":301,"qty":2,"device":{"os":"Android","type":"mobile"}}'),
  (18, 8, TIMESTAMP'2024-06-12 15:30:00', 'search',      '{"page":"/search","query":"tai nghe","results":0,"device":{"os":"Android","type":"mobile"}}'),
  (19, 8, TIMESTAMP'2024-06-12 15:45:00', 'product_view','{"page":"/p/101","product_id":101,"device":{"os":"Android","type":"mobile"}}'),
  (20, 8, TIMESTAMP'2024-06-12 15:58:00', 'checkout',    '{"page":"/checkout","cart_value":3200000,"device":{"os":"Android","type":"mobile"}}'),
  (21, NULL, TIMESTAMP'2024-06-13 01:00:00', 'page_view', '{"page":"/home","referrer":"bot","device":{"os":"Linux","type":"crawler"}}'),
  (22, 9, TIMESTAMP'2024-06-20 21:00:00', 'page_view',   'not-valid-json'),
  (23, 9, TIMESTAMP'2024-06-20 21:10:00', 'product_view','{"page":"/p/201","product_id":201,"device":{"os":"iOS","type":"tablet"}}'),
  (24, 9, TIMESTAMP'2024-06-20 21:28:00', 'checkout',    '{"page":"/checkout","cart_value":535,"device":{"os":"iOS","type":"tablet"}}')
AS web_events(event_id, customer_id, event_ts, event_type, payload);

-- @id: setup:promotions
CREATE OR REPLACE TEMP VIEW promotions AS
SELECT * FROM VALUES
  ('NEWYEAR24', DATE'2024-01-01', DATE'2024-01-07', array('electronics','books'), 0.10),
  ('VALENTINE', DATE'2024-02-10', DATE'2024-02-15', array('beauty','fashion'),    0.15),
  ('SONGKRAN',  DATE'2024-04-10', DATE'2024-04-16', array('home','fashion'),      0.12),
  ('MIDYEAR',   DATE'2024-06-01', DATE'2024-07-15', array(),                      0.08)
AS promotions(promo_code, start_date, end_date, eligible_categories, promo_pct);


-- =====================================================================================
-- Q01 — Web sessionization (30-min inactivity gap) + JSON payload parsing
-- =====================================================================================
-- @id: Q01
WITH parsed AS (
  SELECT
    e.event_id,
    e.customer_id,
    e.event_ts,
    e.event_type,
    from_json(
      e.payload,
      'page STRING, referrer STRING, query STRING, results INT, product_id INT, qty INT,
       cart_value DOUBLE, device STRUCT<os: STRING, type: STRING>'
    )                                               AS p,
    get_json_object(e.payload, '$.device.os')       AS device_os_raw
  FROM web_events e
  WHERE e.customer_id IS NOT NULL
),
gaps AS (
  SELECT
    *,
    LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts, event_id) AS prev_ts,
    CASE
      WHEN LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts, event_id) IS NULL THEN 1
      WHEN unix_timestamp(event_ts)
           - unix_timestamp(LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts, event_id)) > 30 * 60 THEN 1
      ELSE 0
    END AS is_new_session
  FROM parsed
),
sessionized AS (
  SELECT
    *,
    SUM(is_new_session) OVER (
      PARTITION BY customer_id ORDER BY event_ts, event_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_seq
  FROM gaps
)
SELECT
  customer_id,
  concat_ws('-', 'S', lpad(CAST(customer_id AS STRING), 4, '0'), CAST(session_seq AS STRING)) AS session_key,
  MIN(event_ts)                                                       AS session_start,
  MAX(event_ts)                                                       AS session_end,
  round((unix_timestamp(MAX(event_ts)) - unix_timestamp(MIN(event_ts))) / 60.0, 1) AS duration_min,
  COUNT(*)                                                            AS events,
  COUNT(DISTINCT event_type)                                          AS distinct_event_types,
  collect_list(event_type)                                            AS event_path,
  first(p.referrer, true)                                             AS entry_referrer,
  coalesce(max(p.device.type), 'unknown')                             AS device_type,
  max(CASE WHEN event_type = 'checkout' THEN p.cart_value END)        AS checkout_value,
  max(CASE WHEN p IS NULL OR p.page IS NULL THEN 1 ELSE 0 END) = 1     AS has_bad_payload,
  array_join(array_distinct(collect_list(p.query)), ' | ')            AS search_terms
FROM sessionized
GROUP BY customer_id, session_seq
ORDER BY customer_id, session_start;


-- =====================================================================================
-- Q02 — RFM segmentation with NTILE, PERCENT_RANK and percentile_approx
-- =====================================================================================
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


-- =====================================================================================
-- Q03 — PIVOT: monthly net revenue and units by category
-- =====================================================================================
-- @id: Q03
SELECT *
FROM (
  SELECT
    p.category,
    date_format(o.order_ts, 'yyyy-MM')                              AS order_month,
    oi.quantity * oi.unit_price * (1 - oi.discount_pct)             AS net_local,
    oi.quantity                                                     AS units
  FROM order_items oi
  JOIN orders   o ON o.order_id  = oi.order_id
  JOIN products p ON p.product_id = oi.product_id
  WHERE o.status IN ('DELIVERED', 'SHIPPED')
) src
PIVOT (
  round(SUM(net_local), 2) AS rev,
  SUM(units)               AS units
  FOR order_month IN (
    '2024-01' AS jan, '2024-02' AS feb, '2024-03' AS mar, '2024-04' AS apr,
    '2024-05' AS may, '2024-06' AS jun, '2024-07' AS jul
  )
)
ORDER BY category;


-- =====================================================================================
-- Q04 — GROUPING SETS / ROLLUP / CUBE with grouping_id and FILTER clause
-- =====================================================================================
-- @id: Q04a
WITH base AS (
  SELECT
    c.country_code,
    o.channel,
    p.category,
    o.status,
    oi.quantity,
    oi.quantity * oi.unit_price * (1 - oi.discount_pct) AS net_local
  FROM orders o
  JOIN customers   c  ON c.customer_id = o.customer_id
  JOIN order_items oi ON oi.order_id  = o.order_id
  JOIN products    p  ON p.product_id = oi.product_id
)
SELECT
  CASE WHEN grouping(country_code) = 1 THEN '(all countries)' ELSE country_code END AS country_code,
  CASE WHEN grouping(channel)      = 1 THEN '(all channels)'  ELSE channel      END AS channel,
  CASE WHEN grouping(category)     = 1 THEN '(all categories)' ELSE category    END AS category,
  grouping_id(country_code, channel, category)                                AS gid,
  COUNT(*)                                                                    AS lines,
  SUM(quantity)                                                               AS units,
  SUM(quantity) FILTER (WHERE status = 'DELIVERED')                           AS units_delivered,
  COUNT(*)      FILTER (WHERE status IN ('CANCELLED', 'RETURNED'))            AS lines_lost,
  round(SUM(net_local) FILTER (WHERE status <> 'CANCELLED'), 2)               AS net_local_ex_cancel
FROM base
GROUP BY GROUPING SETS (
  (country_code, channel, category),
  (country_code, channel),
  (country_code),
  (channel, category),
  ()
)
ORDER BY gid, country_code, channel, category;

-- Q04b — ROLLUP and CUBE variants (converter should handle both shorthand forms)
-- @id: Q04b
SELECT c.country_code, o.channel, COUNT(DISTINCT o.order_id) AS orders, grouping_id() AS gid
FROM orders o JOIN customers c ON c.customer_id = o.customer_id
GROUP BY ROLLUP (c.country_code, o.channel)
ORDER BY gid, country_code, channel;

-- @id: Q04c
SELECT o.channel, o.status, COUNT(*) AS orders
FROM orders o
GROUP BY CUBE (o.channel, o.status)
HAVING COUNT(*) > 1
ORDER BY channel NULLS FIRST, status NULLS FIRST;


-- =====================================================================================
-- Q05 — Arrays, maps, structs and higher-order functions; LATERAL VIEW explode
-- =====================================================================================
-- @id: Q05a
WITH prod_attr AS (
  SELECT
    product_id,
    product_name,
    category,
    from_json(
      attributes_json,
      'color STRING, weight_g INT, sizes ARRAY<STRING>, tags ARRAY<STRING>,
       warranty STRUCT<months: INT>, volume_ml INT, pages INT'
    ) AS a
  FROM products
),
cust_profile AS (
  SELECT
    c.customer_id,
    c.full_name,
    c.interests,
    c.preferences,
    collect_list(named_struct(
      'order_id', o.order_id,
      'ts',       o.order_ts,
      'status',   o.status,
      'city',     o.ship_to.city
    )) AS order_structs
  FROM customers c
  LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.full_name, c.interests, c.preferences
)
SELECT
  cp.customer_id,
  cp.full_name,
  size(cp.interests)                                                         AS n_interests,
  transform(cp.interests, x -> initcap(x))                                   AS interests_title,
  filter(cp.interests, x -> x IN ('electronics', 'books'))                   AS tech_or_books,
  exists(cp.interests, x -> x = 'beauty')                                    AS likes_beauty,
  forall(cp.order_structs, s -> s.status IN ('DELIVERED','SHIPPED') OR s.order_id IS NULL) AS all_orders_ok,
  aggregate(
    transform(filter(cp.order_structs, s -> s.order_id IS NOT NULL), s -> 1),
    0, (acc, x) -> acc + x
  )                                                                          AS order_count_hof,
  -- get() returns NULL on an empty array; arr[0] would throw under ANSI mode
  get(array_sort(
    filter(cp.order_structs, s -> s.order_id IS NOT NULL),
    (l, r) -> CASE WHEN l.ts > r.ts THEN -1 WHEN l.ts < r.ts THEN 1 ELSE 0 END
  ), 0).order_id                                                             AS latest_order_id,
  array_distinct(transform(filter(cp.order_structs, s -> s.city IS NOT NULL), s -> s.city)) AS ship_cities,
  map_filter(cp.preferences, (k, v) -> v = 'Y')                              AS opted_in_channels,
  transform_values(cp.preferences, (k, v) -> v = 'Y')                        AS prefs_bool,
  map_keys(cp.preferences)                                                   AS pref_keys,
  element_at(cp.preferences, 'newsletter')                                   AS newsletter_flag,
  zip_with(
    cp.interests,
    sequence(1, size(cp.interests)),
    (i, n) -> concat(CAST(n AS STRING), ':', i)
  )                                                                          AS ranked_interests
FROM cust_profile cp
ORDER BY cp.customer_id;

-- Q05b — LATERAL VIEW posexplode / explode OUTER / inline over structs
-- @id: Q05b
SELECT
  pa.product_id,
  pa.product_name,
  t.pos      AS tag_pos,
  t.tag,
  sz.size_label,
  pa.a.warranty.months                                      AS warranty_months,
  coalesce(pa.a.weight_g, -1)                               AS weight_g
FROM (
  SELECT product_id, product_name,
         from_json(attributes_json,
           'color STRING, weight_g INT, sizes ARRAY<STRING>, tags ARRAY<STRING>, warranty STRUCT<months: INT>') AS a
  FROM products
) pa
LATERAL VIEW OUTER posexplode(pa.a.tags)  t  AS pos, tag
LATERAL VIEW OUTER explode(pa.a.sizes)    sz AS size_label
ORDER BY pa.product_id, tag_pos, size_label;

-- @id: Q05c
SELECT customer_id, o.*
FROM (
  SELECT customer_id,
         array(
           named_struct('k', 'first_order',  'v', CAST(MIN(order_ts) AS STRING)),
           named_struct('k', 'last_order',   'v', CAST(MAX(order_ts) AS STRING)),
           named_struct('k', 'order_count',  'v', CAST(COUNT(*) AS STRING))
         ) AS kv
  FROM orders
  GROUP BY customer_id
) x
LATERAL VIEW inline(x.kv) o AS k, v
ORDER BY customer_id, k;


-- =====================================================================================
-- Q06 — Semi/anti joins and correlated subqueries
-- =====================================================================================
-- Customers who bought electronics AND have never had a cancelled/returned order,
-- with a correlated scalar subquery for their single largest line.
-- @id: Q06a
SELECT
  c.customer_id,
  c.full_name,
  c.country_code,
  (SELECT MAX(oi.quantity * oi.unit_price)
     FROM orders o2
     JOIN order_items oi ON oi.order_id = o2.order_id
    WHERE o2.customer_id = c.customer_id)                                   AS max_line_local,
  (SELECT COUNT(*) FROM web_events w WHERE w.customer_id = c.customer_id)   AS web_events
FROM customers c
LEFT SEMI JOIN (
  SELECT o.customer_id
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  JOIN products p     ON p.product_id = oi.product_id
  WHERE p.category = 'electronics'
) elec ON elec.customer_id = c.customer_id
WHERE NOT EXISTS (
        SELECT 1 FROM orders ox
        WHERE ox.customer_id = c.customer_id
          AND ox.status IN ('CANCELLED', 'RETURNED')
      )
ORDER BY c.customer_id;

-- Dormant customers: signed up, never ordered (LEFT ANTI), but have preferences set
-- @id: Q06b
SELECT c.customer_id, c.full_name, c.signup_date, size(map_keys(c.preferences)) AS n_prefs
FROM customers c
LEFT ANTI JOIN orders o ON o.customer_id = c.customer_id
WHERE c.signup_date < DATE'2024-01-01';

-- Products never sold, or sold only in orders that were later cancelled
-- @id: Q06c
SELECT p.product_id, p.sku, p.product_name
FROM products p
WHERE p.product_id NOT IN (
        SELECT oi.product_id
        FROM order_items oi
        WHERE oi.order_id IN (SELECT order_id FROM orders WHERE status <> 'CANCELLED')
      )
   OR EXISTS (
        SELECT 1 FROM order_items oi2
        WHERE oi2.product_id = p.product_id AND oi2.unit_price < p.list_price * 2
      )
ORDER BY p.product_id;


-- =====================================================================================
-- Q07 — Set operations
-- =====================================================================================
-- @id: Q07
WITH web_buyers  AS (SELECT DISTINCT customer_id FROM orders WHERE channel = 'web'),
     app_buyers  AS (SELECT DISTINCT customer_id FROM orders WHERE channel = 'app'),
     browsers    AS (SELECT DISTINCT customer_id FROM web_events WHERE customer_id IS NOT NULL)
SELECT 'web_and_app'        AS cohort, customer_id FROM (SELECT customer_id FROM web_buyers INTERSECT SELECT customer_id FROM app_buyers)
UNION ALL
SELECT 'web_only'           AS cohort, customer_id FROM (SELECT customer_id FROM web_buyers EXCEPT SELECT customer_id FROM app_buyers)
UNION ALL
SELECT 'app_only'           AS cohort, customer_id FROM (SELECT customer_id FROM app_buyers EXCEPT SELECT customer_id FROM web_buyers)
UNION ALL
SELECT 'browsed_no_purchase' AS cohort, customer_id FROM (
  SELECT customer_id FROM browsers
  EXCEPT
  SELECT customer_id FROM orders WHERE status IN ('DELIVERED', 'SHIPPED')
)
UNION
SELECT 'any_activity'       AS cohort, customer_id FROM (
  -- inner UNION ALL keeps duplicates on purpose: only the outer UNION removes them
  SELECT customer_id FROM browsers UNION ALL SELECT customer_id FROM orders
)
ORDER BY cohort, customer_id;


-- =====================================================================================
-- Q08 — Point-in-time FX conversion + time-range window frames
-- =====================================================================================
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


-- =====================================================================================
-- Q09 — Funnel analysis per customer-day
-- =====================================================================================
-- @id: Q09a
WITH ev AS (
  SELECT customer_id, to_date(event_ts) AS d, event_ts, event_type
  FROM web_events
  WHERE customer_id IS NOT NULL
),
funnel AS (
  SELECT
    customer_id,
    d,
    MIN(CASE WHEN event_type IN ('page_view','search') THEN event_ts END) AS t_entry,
    MIN(CASE WHEN event_type = 'product_view'  THEN event_ts END)         AS t_view,
    MIN(CASE WHEN event_type = 'add_to_cart'   THEN event_ts END)         AS t_cart,
    MIN(CASE WHEN event_type = 'checkout'      THEN event_ts END)         AS t_checkout,
    concat_ws(' > ', array_distinct(collect_list(event_type)))            AS path
  FROM ev
  GROUP BY customer_id, d
)
SELECT
  f.customer_id,
  f.d,
  f.path,
  CASE
    WHEN f.t_checkout IS NOT NULL THEN '4_checkout'
    WHEN f.t_cart     IS NOT NULL THEN '3_cart'
    WHEN f.t_view     IS NOT NULL THEN '2_view'
    ELSE '1_entry'
  END                                                                    AS deepest_stage,
  (unix_timestamp(f.t_checkout) - unix_timestamp(coalesce(f.t_entry, f.t_view))) / 60 AS minutes_to_checkout,
  o.order_id                                                             AS matched_order,
  array_contains(split(f.path, ' > '), 'search')                         AS used_search
FROM funnel f
LEFT JOIN orders o
  ON o.customer_id = f.customer_id
 AND o.order_ts BETWEEN f.t_checkout AND f.t_checkout + INTERVAL 10 MINUTES
ORDER BY f.customer_id, f.d;

-- Funnel summary with conversion rates
-- @id: Q09b
WITH stage_flags AS (
  SELECT
    customer_id, to_date(event_ts) AS d,
    max(event_type = 'product_view') AS viewed,
    max(event_type = 'add_to_cart')  AS carted,
    max(event_type = 'checkout')     AS checked_out
  FROM web_events
  WHERE customer_id IS NOT NULL
  GROUP BY customer_id, to_date(event_ts)
)
SELECT
  COUNT(*)                                                     AS visit_days,
  count_if(viewed)                                             AS view_days,
  count_if(carted)                                             AS cart_days,
  count_if(checked_out)                                        AS checkout_days,
  round(try_divide(count_if(carted), count_if(viewed)) * 100, 1)       AS view_to_cart_pct,
  round(try_divide(count_if(checked_out), count_if(carted)) * 100, 1)  AS cart_to_checkout_pct
FROM stage_flags;


-- =====================================================================================
-- Q10 — UNPIVOT (Spark 3.4+) and the stack() equivalent
-- =====================================================================================
-- @id: Q10a
WITH monthly AS (
  SELECT
    customer_id,
    SUM(CASE WHEN month(order_ts) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS q1_orders,
    SUM(CASE WHEN month(order_ts) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS q2_orders,
    SUM(CASE WHEN month(order_ts) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS q3_orders
  FROM orders
  GROUP BY customer_id
)
SELECT customer_id, quarter, n_orders
FROM monthly
UNPIVOT EXCLUDE NULLS (n_orders FOR quarter IN (q1_orders AS Q1, q2_orders AS Q2, q3_orders AS Q3))
WHERE n_orders > 0
ORDER BY customer_id, quarter;

-- @id: Q10b
SELECT customer_id, stack(3, 'Q1', q1_orders, 'Q2', q2_orders, 'Q3', q3_orders) AS (quarter, n_orders)
FROM (
  SELECT
    customer_id,
    SUM(CASE WHEN quarter(order_ts) = 1 THEN 1 ELSE 0 END) AS q1_orders,
    SUM(CASE WHEN quarter(order_ts) = 2 THEN 1 ELSE 0 END) AS q2_orders,
    SUM(CASE WHEN quarter(order_ts) = 3 THEN 1 ELSE 0 END) AS q3_orders
  FROM orders
  GROUP BY customer_id
) m
ORDER BY customer_id, quarter;


-- =====================================================================================
-- Q11 — Market-basket pairs (self-join), CROSS JOIN calendar, FULL OUTER JOIN
-- =====================================================================================
-- @id: Q11a
WITH basket AS (
  SELECT DISTINCT oi.order_id, p.category
  FROM order_items oi
  JOIN products p ON p.product_id = oi.product_id
  JOIN orders   o ON o.order_id = oi.order_id
  WHERE o.status <> 'CANCELLED'
),
pairs AS (
  SELECT a.category AS cat_a, b.category AS cat_b, COUNT(DISTINCT a.order_id) AS together
  FROM basket a
  JOIN basket b
    ON a.order_id = b.order_id
   AND a.category < b.category
  GROUP BY a.category, b.category
),
singles AS (
  SELECT category, COUNT(DISTINCT order_id) AS n FROM basket GROUP BY category
),
total AS (SELECT COUNT(DISTINCT order_id) AS n_orders FROM basket)
SELECT
  pr.cat_a, pr.cat_b, pr.together,
  round(pr.together / t.n_orders, 3)                           AS support,
  round(pr.together / sa.n, 3)                                 AS confidence_a_to_b,
  round((pr.together / t.n_orders) / ((sa.n / t.n_orders) * (sb.n / t.n_orders)), 3) AS lift
FROM pairs pr
JOIN singles sa ON sa.category = pr.cat_a
JOIN singles sb ON sb.category = pr.cat_b
CROSS JOIN total t
ORDER BY lift DESC, together DESC;

-- Calendar spine × channel, FULL OUTER JOIN with actuals to expose gaps on both sides
-- @id: Q11b
WITH months AS (
  SELECT explode(sequence(DATE'2024-01-01', DATE'2024-08-01', INTERVAL 1 MONTH)) AS month_start
),
channels AS (SELECT DISTINCT channel FROM orders),
spine AS (SELECT m.month_start, c.channel FROM months m CROSS JOIN channels c),
actuals AS (
  SELECT CAST(date_trunc('MONTH', order_ts) AS DATE) AS month_start, channel, COUNT(*) AS orders
  FROM orders
  GROUP BY 1, 2
)
SELECT
  coalesce(s.month_start, a.month_start) AS month_start,
  coalesce(s.channel, a.channel)         AS channel,
  coalesce(a.orders, 0)                  AS orders,
  s.month_start IS NULL                  AS outside_spine
FROM spine s
FULL OUTER JOIN actuals a
  ON a.month_start = s.month_start AND a.channel = s.channel
ORDER BY month_start, channel;


-- =====================================================================================
-- Q12 — THE BIG ONE: customer 360 scorecard
--        14 chained CTEs combining sessions, FX as-of joins, promotions (array join
--        condition), payments reconciliation, HOFs, windows, and a final ranked output.
-- =====================================================================================
-- @id: Q12
WITH
-- 1. Clean customers: normalize email/name, derive tenure
c_clean AS (
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
  FROM customers
),
-- 2. Product dimension with parsed JSON attributes
p_dim AS (
  SELECT
    product_id, sku, product_name, category,
    coalesce(sub_category, 'n/a')                                     AS sub_category,
    coalesce(brand, 'Unbranded')                                      AS brand,
    list_price,
    CAST(get_json_object(attributes_json, '$.warranty.months') AS INT) AS warranty_months,
    from_json(get_json_object(attributes_json, '$.tags'), 'ARRAY<STRING>') AS tags
  FROM products
),
-- 3. Line-level facts with gross / discount / net in local currency
lines AS (
  SELECT
    oi.order_id, oi.line_no, oi.product_id, oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price                                       AS gross_local,
    oi.quantity * oi.unit_price * oi.discount_pct                     AS discount_local,
    oi.quantity * oi.unit_price * (1 - oi.discount_pct)               AS net_local
  FROM order_items oi
),
-- 4. Orders enriched with line aggregates and categories
o_enriched AS (
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
  GROUP BY o.order_id, o.customer_id, o.order_ts, o.status, o.channel, o.currency, o.ship_to.city
),
-- 5. As-of FX rates
fx_asof AS (
  SELECT order_id, rate_to_usd FROM (
    SELECT /*+ BROADCAST(f) */
      oe.order_id, f.rate_to_usd,
      ROW_NUMBER() OVER (PARTITION BY oe.order_id ORDER BY f.rate_date DESC) AS rn
    FROM o_enriched oe
    JOIN fx_rates f ON f.currency = oe.currency AND f.rate_date <= oe.order_date
  ) WHERE rn = 1
),
-- 6. Payments reconciliation per order
pay AS (
  SELECT
    order_id,
    SUM(CASE WHEN payment_status = 'CAPTURED' THEN amount ELSE 0 END)  AS captured_local,
    SUM(CASE WHEN payment_status = 'REFUNDED' THEN amount ELSE 0 END)  AS refunded_local,
    COUNT(DISTINCT method)                                             AS n_methods,
    concat_ws('+', array_sort(collect_set(method)))                    AS methods,
    max(paid_ts)                                                       AS last_paid_ts,
    bool_or(payment_status = 'FAILED')                                 AS any_failed
  FROM payments
  GROUP BY order_id
),
-- 7. Promotions applicable to each order (date range + array overlap)
promo_hits AS (
  SELECT
    oe.order_id,
    max_by(pr.promo_code, pr.promo_pct)                                AS best_promo,
    max(pr.promo_pct)                                                  AS best_promo_pct
  FROM o_enriched oe
  JOIN promotions pr
    ON oe.order_date BETWEEN pr.start_date AND pr.end_date
   AND (size(pr.eligible_categories) = 0 OR arrays_overlap(pr.eligible_categories, oe.categories))
  GROUP BY oe.order_id
),
-- 8. Order fact in USD with reconciliation flags
o_fact AS (
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
  LEFT JOIN promo_hits ph  ON ph.order_id  = oe.order_id
),
-- 9. Order sequencing per customer
o_seq AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER w_cust                                            AS order_seq,
    COUNT(*)     OVER (PARTITION BY customer_id)                        AS cust_order_cnt,
    datediff(order_date, LAG(order_date) OVER w_cust)                   AS gap_days,
    SUM(CASE WHEN status IN ('DELIVERED','SHIPPED') THEN net_usd ELSE 0 END)
      OVER (PARTITION BY customer_id ORDER BY order_ts, order_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS cum_kept_usd
  FROM o_fact f
  WINDOW w_cust AS (PARTITION BY customer_id ORDER BY order_ts, order_id)
),
-- 10. Sessions (re-uses Q01 logic, compressed)
sess AS (
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
  GROUP BY customer_id, session_seq
),
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
