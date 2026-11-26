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


-- =====================================================================================
--  sql2pyspark — Spark SQL conversion test suite, PART 2  (Q13 – Q26)
--  ---------------------------------------------------------------------------------
--  Extends Part 1 (sql2pyspark_test_suite.sql, Q01–Q12). Part 2 re-uses Part 1's
--  setup views (customers, products, orders, order_items, payments, fx_rates,
--  web_events) and adds five more below. Run both parts as one suite:
--
--      cat sql2pyspark_test_suite.sql sql2pyspark_test_suite_part2.sql > suite_all.sql
--      python compare_harness.py --sql suite_all.sql --conversions conversions
--
--  Same "-- @id: <ID>" convention, so compare_harness.py needs no changes.
--
--  Target: Q13–Q24 and Q26 avoid 4.x-only syntax and are intended for Spark 3.5+.
--  Q25 is the Spark 4.x section (WITH RECURSIVE, pipe syntax, QUALIFY, collations,
--  VARIANT, listagg). All statements were run on Spark 4.3.0 only, with ANSI mode on
--  and session time zone UTC (the harness default); results are identical across
--  1, 4 and 9 shuffle partitions.
--
--  Feature coverage (new in Part 2):
--    Q13  Gaps-and-islands: running stock balance, ROW_NUMBER-difference islands,
--         date_sub(d, rank) streaks, LEAD to close intervals, bool_or, max_by(struct)
--    Q14  FIFO inventory costing: cumulative-range overlap join, least/greatest,
--         layer arrays, ROLLUP valuation with grouping_id
--    Q15  SCD Type 2: IS DISTINCT FROM de-dup, LEAD(.., 1, default), half-open as-of
--         join, null-safe <=>, inline VALUES join, correlated scalar count, map_from_entries
--    Q16  Hierarchy flattening with fixed-depth self-joins, element_at(arr, -2),
--         explode ancestor paths for roll-up, NOT EXISTS leaf test, sibling ranks
--    Q17  Text: regexp_extract_all, look-ahead masking, back-references, str_to_map,
--         parse_url, LIKE ANY / ILIKE / RLIKE / ESCAPE, length vs octet_length (Thai),
--         levenshtein, sha2, format_string, translate, json-free key=value parsing
--    Q18  Time: window() with slide + startTime, session_window with dynamic gap,
--         from_utc_timestamp, business-minutes via sequence + HOF, timestampdiff, next_day
--    Q19  Gap-filling: date spine via sequence, forward/back fill with both IGNORE NULLS
--         syntaxes, linear interpolation, RANGE INTERVAL frames on DATE vs ROWS frames
--    Q20  Stats: percentile_cont / percentile_disc WITHIN GROUP, median, exact percentile,
--         regr_*, corr, covar, skewness, kurtosis, COUNT(DISTINCT a, b), GROUP BY ALL,
--         width_bucket, window over an aggregate in the same SELECT
--    Q21  Arrays/maps deep-dive: slice, negative element_at, try_element_at, arrays_zip,
--         array_position, index-aware transform/filter, reduce with finish, struct
--         accumulators, map_zip_with, transform_keys, map_concat, three-valued exists,
--         array_compact/append/prepend/insert, LATERAL VIEW explode(map), inline_outer
--    Q22  Subqueries: correlated LATERAL with aggregates (chained), EXISTS in SELECT and
--         CASE, tuple IN subquery, correlated AVG comparison, scalar subquery in HAVING,
--         CTE nested inside a subquery, INTERSECT ALL / EXCEPT ALL
--    Q23  NULL and numeric semantics: NOT IN with a NULL (empty result!), three-valued
--         logic, decimal result types, int / int = double, div, %, pmod, bround,
--         ceil(x, d), try_* functions, ANSI casts, typeof() columns
--    Q24  Multi-column PIVOT with multiple aggregates, multi-value UNPIVOT INCLUDE NULLS,
--         transpose via stack() + PIVOT
--    Q25  Spark 4.x: WITH RECURSIVE, SQL pipe syntax |>, QUALIFY, LATERAL ... ORDER BY
--         LIMIT, listagg WITHIN GROUP, COLLATE UTF8_LCASE, parse_json / variant_get
--    Q26  THE BIG ONE #2: order margin control tower — 17 CTEs combining FIFO COGS,
--         SCD2 tiers, hierarchy paths, ticket text mining, SLA, FX as-of, hints,
--         DISTRIBUTE BY / SORT BY, and a final multi-window scoring
-- =====================================================================================


-- =====================================================================================
-- SECTION 0b — ADDITIONAL SOURCE DATA (Part 1's setup views must already exist)
-- =====================================================================================

-- Inventory movements. qty is always positive; move_type gives the direction.
-- unit_cost (USD) is set on inbound moves only. Moves 31/32 share a timestamp on
-- purpose (ties must be broken by move_id). Move 36 oversells product 501 (backorder),
-- move 38 writes off stock that never existed.
-- @id: setup:inventory_movements
CREATE OR REPLACE TEMP VIEW inventory_movements AS
SELECT * FROM VALUES
  (1,  101, 'WH-BKK', TIMESTAMP'2024-01-01 08:00:00', 'RECEIPT',    10, CAST(80.00 AS DECIMAL(10,2))),
  (2,  101, 'WH-BKK', TIMESTAMP'2024-01-03 10:20:00', 'SALE',        3, CAST(NULL  AS DECIMAL(10,2))),
  (3,  101, 'WH-BKK', TIMESTAMP'2024-02-01 08:00:00', 'RECEIPT',     5, CAST(85.00 AS DECIMAL(10,2))),
  (4,  101, 'WH-BKK', TIMESTAMP'2024-02-14 21:00:00', 'SALE',        6, CAST(NULL  AS DECIMAL(10,2))),
  (5,  101, 'WH-BKK', TIMESTAMP'2024-03-01 12:30:00', 'SALE',        4, CAST(NULL  AS DECIMAL(10,2))),
  (6,  101, 'WH-BKK', TIMESTAMP'2024-03-15 09:00:00', 'SALE',        2, CAST(NULL  AS DECIMAL(10,2))),
  (7,  101, 'WH-BKK', TIMESTAMP'2024-04-01 08:00:00', 'RECEIPT',     8, CAST(78.00 AS DECIMAL(10,2))),
  (8,  101, 'WH-BKK', TIMESTAMP'2024-06-11 11:30:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (9,  101, 'WH-BKK', TIMESTAMP'2024-06-12 16:30:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (10, 101, 'WH-BKK', TIMESTAMP'2024-07-01 18:00:00', 'ADJUST_OUT',  1, CAST(NULL  AS DECIMAL(10,2))),
  (11, 101, 'WH-SGP', TIMESTAMP'2024-02-20 08:00:00', 'RECEIPT',     4, CAST(82.00 AS DECIMAL(10,2))),
  (12, 101, 'WH-SGP', TIMESTAMP'2024-03-01 12:10:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (13, 101, 'WH-SGP', TIMESTAMP'2024-07-04 13:30:00', 'SALE',        3, CAST(NULL  AS DECIMAL(10,2))),
  (14, 202, 'WH-BKK', TIMESTAMP'2024-01-10 08:00:00', 'RECEIPT',     6, CAST(40.00 AS DECIMAL(10,2))),
  (15, 202, 'WH-BKK', TIMESTAMP'2024-02-15 08:10:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (16, 202, 'WH-BKK', TIMESTAMP'2024-02-16 10:00:00', 'ADJUST_IN',   1, CAST(40.00 AS DECIMAL(10,2))),
  (17, 202, 'WH-BKK', TIMESTAMP'2024-05-05 19:10:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (18, 202, 'WH-BKK', TIMESTAMP'2024-07-15 18:30:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (19, 202, 'WH-BKK', TIMESTAMP'2024-07-16 08:00:00', 'RECEIPT',    10, CAST(42.50 AS DECIMAL(10,2))),
  (20, 302, 'WH-JKT', TIMESTAMP'2024-03-01 08:00:00', 'RECEIPT',     3, CAST(95.00 AS DECIMAL(10,2))),
  (21, 302, 'WH-JKT', TIMESTAMP'2024-03-20 23:59:59', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (22, 302, 'WH-JKT', TIMESTAMP'2024-04-18 14:30:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (23, 302, 'WH-JKT', TIMESTAMP'2024-05-01 08:00:00', 'RECEIPT',     2, CAST(99.00 AS DECIMAL(10,2))),
  (24, 302, 'WH-JKT', TIMESTAMP'2024-07-01 10:05:00', 'SALE',        2, CAST(NULL  AS DECIMAL(10,2))),
  (25, 302, 'WH-JKT', TIMESTAMP'2024-07-02 08:00:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (26, 401, 'WH-BKK', TIMESTAMP'2024-01-15 08:00:00', 'RECEIPT',    20, CAST(6.50  AS DECIMAL(10,2))),
  (27, 401, 'WH-BKK', TIMESTAMP'2024-02-14 21:00:00', 'SALE',        3, CAST(NULL  AS DECIMAL(10,2))),
  (28, 401, 'WH-BKK', TIMESTAMP'2024-04-02 09:40:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (29, 401, 'WH-BKK', TIMESTAMP'2024-05-05 19:10:00', 'SALE',        2, CAST(NULL  AS DECIMAL(10,2))),
  (30, 401, 'WH-BKK', TIMESTAMP'2024-06-20 21:40:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (31, 401, 'WH-BKK', TIMESTAMP'2024-06-30 17:00:00', 'ADJUST_OUT',  2, CAST(NULL  AS DECIMAL(10,2))),
  (32, 401, 'WH-BKK', TIMESTAMP'2024-06-30 17:00:00', 'RECEIPT',    12, CAST(7.10  AS DECIMAL(10,2))),
  (33, 501, 'WH-BKK', TIMESTAMP'2024-02-01 08:00:00', 'RECEIPT',     2, CAST(30.00 AS DECIMAL(10,2))),
  (34, 501, 'WH-BKK', TIMESTAMP'2024-02-14 21:00:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (35, 501, 'WH-BKK', TIMESTAMP'2024-05-30 08:00:00', 'SALE',        1, CAST(NULL  AS DECIMAL(10,2))),
  (36, 501, 'WH-BKK', TIMESTAMP'2024-07-04 13:30:00', 'SALE',        2, CAST(NULL  AS DECIMAL(10,2))),
  (37, 501, 'WH-BKK', TIMESTAMP'2024-07-20 08:00:00', 'RECEIPT',     5, CAST(31.50 AS DECIMAL(10,2))),
  (38, 103, 'WH-BKK', TIMESTAMP'2024-07-10 09:00:00', 'ADJUST_OUT',  1, CAST(NULL  AS DECIMAL(10,2)))
AS inventory_movements(move_id, product_id, warehouse, move_ts, move_type, qty, unit_cost);

-- Tier history (SCD2 source). Contains no-op changes (3: PLATINUM->PLATINUM,
-- 5: NULL->NULL) that only a null-safe comparison removes, and a downgrade (7).
-- @id: setup:tier_history
CREATE OR REPLACE TEMP VIEW tier_history AS
SELECT * FROM VALUES
  (1, 'BRONZE',   DATE'2022-01-15', 'signup'), (1, 'SILVER',   DATE'2023-03-01', 'auto'),  (1, 'GOLD', DATE'2024-02-01', 'auto'),
  (2, 'BRONZE',   DATE'2022-03-02', 'signup'), (2, 'SILVER',   DATE'2024-01-01', 'auto'),
  (3, 'GOLD',     DATE'2021-11-20', 'signup'), (3, 'PLATINUM', DATE'2023-06-01', 'auto'),  (3, 'PLATINUM', DATE'2024-03-01', 'manual'),
  (4, 'BRONZE',   DATE'2023-06-10', 'signup'),
  (5, CAST(NULL AS STRING), DATE'2023-07-01', 'signup'), (5, CAST(NULL AS STRING), DATE'2023-12-01', 'auto'), (5, 'BRONZE', DATE'2024-05-01', 'manual'),
  (6, 'BRONZE',   DATE'2024-02-29', 'signup'), (6, 'SILVER',   DATE'2024-06-01', 'auto'),
  (7, 'SILVER',   DATE'2022-08-08', 'signup'), (7, 'GOLD',     DATE'2024-01-01', 'auto'),  (7, 'SILVER', DATE'2024-05-15', 'auto'), (7, 'GOLD', DATE'2024-06-15', 'manual'),
  (8, 'BRONZE',   DATE'2024-05-05', 'signup'),
  (9, 'PLATINUM', DATE'2021-01-01', 'signup')
AS tier_history(customer_id, tier, effective_from, change_source);

-- Category hierarchy (depth 0..3). Leaves match products.sub_category within the
-- parent matching products.category. Node 99 is an orphan (parent 98 doesn't exist);
-- node 312 is a leaf with no products; node 40 is a childless level-1 node.
-- @id: setup:category_tree
CREATE OR REPLACE TEMP VIEW category_tree AS
SELECT * FROM VALUES
  (1,   CAST(NULL AS INT), 'All Products'),
  (10,  1,  'Consumer Tech'), (11, 10, 'Electronics'), (111, 11, 'audio'), (112, 11, 'camera'), (113, 11, 'accessory'),
  (20,  1,  'Lifestyle'),
  (21,  20, 'Fashion'),       (211, 21, 'tops'),       (212, 21, 'footwear'),
  (22,  20, 'Home'),          (221, 22, 'kitchen'),    (222, 22, 'appliance'),
  (23,  20, 'Beauty'),        (231, 23, 'skincare'),   (232, 23, 'suncare'),
  (30,  1,  'Media'),         (31,  30, 'Books'),      (311, 31, 'tech'),     (312, 31, 'fiction'),
  (40,  1,  'Clearance'),
  (99,  98, 'Legacy Imports')
AS category_tree(node_id, parent_id, node_name);

-- Support tickets: messy free text (order refs in several spellings, phone numbers,
-- card fragments, a Thai phrase), a blank agent, upper-case agent email, NULL meta.
-- @id: setup:support_tickets
CREATE OR REPLACE TEMP VIEW support_tickets AS
SELECT * FROM VALUES
  ('T-0001', 1,    TIMESTAMP'2024-01-04 02:12:00', TIMESTAMP'2024-01-04 02:40:00', TIMESTAMP'2024-01-04 08:00:00', 'email', 'P3',
   'Earbuds left side quiet',
   'Hi team, order ORD-1001 earbuds left bud is quieter. Call me at +66-81-234-5678 or alice.tan@example.com',
   'audio;defect;warranty', 'nok.s@support.example.com', 'src=web;lang=en;vip=0', 4),
  ('T-0002', 2,    TIMESTAMP'2024-02-15 01:30:00', TIMESTAMP'2024-02-15 01:31:00', TIMESTAMP'2024-02-15 01:45:00', 'chat',  'P2',
   'CANCEL my order!!',
   'Please cancel order #1003 now. Wrong size.',
   'cancel;fashion', 'ploy.k@support.example.com', 'src=app;lang=th;vip=0', 5),
  ('T-0003', 3,    TIMESTAMP'2024-03-21 01:00:00', TIMESTAMP'2024-03-21 10:15:00', TIMESTAMP'2024-03-25 11:00:00', 'email', 'P2',
   'Return request - air purifier',
   'Returning ORD-1005: purifier makes noise.  Refund to card ****4242 please.',
   'return;home;refund', 'nok.s@support.example.com', 'src=email;lang=en;vip=1', 2),
  ('T-0004', 5,    TIMESTAMP'2024-04-20 06:00:00', CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), 'phone', 'P1',
   'Where is my order?',
   'Order 1007 shipped 2 days ago, no tracking. ติดต่อกลับด่วน',
   'shipping;tracking', '   ', 'src=phone;lang=id', CAST(NULL AS INT)),
  ('T-0005', 6,    TIMESTAMP'2024-05-06 03:00:00', TIMESTAMP'2024-05-06 03:05:00', TIMESTAMP'2024-05-06 03:30:00', 'chat',  'P4',
   'serum question',
   'Is the Vitamin C serum vegan? also is the sunscreen reef-safe? thanks',
   'beauty;presales', 'PLOY.K@SUPPORT.EXAMPLE.COM', 'src=app;lang=en;vip=0;utm=https://shop.example.com/p/401?utm_source=line&utm_campaign=midyear', 5),
  ('T-0006', 7,    TIMESTAMP'2024-05-31 15:00:00', TIMESTAMP'2024-06-03 02:00:00', TIMESTAMP'2024-06-04 05:00:00', 'email', 'P3',
   'Book arrived damaged',
   'ORD-1009 book cover torn, photo attached. Please check order ORD-1009 again',
   'books;defect;damage', 'arun.p@support.example.com', 'src=email;lang=en;vip=0', 3),
  ('T-0007', 8,    TIMESTAMP'2024-06-12 09:05:00', TIMESTAMP'2024-06-12 09:06:00', CAST(NULL AS TIMESTAMP), 'chat',  'P1',
   'Payment failed',
   'Payment for order 1011 FAILED twice; card ****1111. Wallet also failed.',
   'payment;failed;urgent', 'arun.p@support.example.com', 'src=app;lang=vi;vip=0', 1),
  ('T-0008', CAST(NULL AS INT), TIMESTAMP'2024-06-12 18:30:00', CAST(NULL AS TIMESTAMP), TIMESTAMP'2024-06-12 18:31:00', 'email', 'P4',
   'RE: RE: FW: newsletter',
   'unsubscribe',
   'spam', 'bot@support.example.com', CAST(NULL AS STRING), CAST(NULL AS INT)),
  ('T-0009', 9,    TIMESTAMP'2024-07-02 02:00:00', TIMESTAMP'2024-07-02 02:30:00', TIMESTAMP'2024-07-03 02:00:00', 'phone', 'P3',
   'Purifier filter',
   'Need replacement filter for ORD-1013 (and maybe ORD-1012?) Thanks',
   'home;presales;warranty', 'ploy.k@support.example.com', 'src=phone;lang=ms;vip=1', 4),
  ('T-0010', 1,    TIMESTAMP'2024-07-05 16:50:00', TIMESTAMP'2024-07-05 17:20:00', TIMESTAMP'2024-07-08 04:11:00', 'email', 'P2',
   'Charger overheating!!!',
   'The 65W charger from ord-1010 gets very hot. Safety issue?',
   'electronics;safety;defect', 'nok.s@support.example.com', 'src=web;lang=en;vip=0', CAST(NULL AS INT))
AS support_tickets(ticket_id, customer_id, opened_ts, first_response_ts, resolved_ts, channel, priority,
                   subject, body, tags, agent_email, meta, csat);

-- Sparse daily price feed (USD) for January 2024. A NULL price means "feed row
-- arrived but empty" and must be skipped by fills. 202 has no price before Jan 3.
-- @id: setup:price_history
CREATE OR REPLACE TEMP VIEW price_history AS
SELECT * FROM VALUES
  (101, DATE'2024-01-01', CAST(129.90 AS DECIMAL(10,2))),
  (101, DATE'2024-01-08', CAST(119.90 AS DECIMAL(10,2))),
  (101, DATE'2024-01-15', CAST(NULL   AS DECIMAL(10,2))),
  (101, DATE'2024-01-20', CAST(124.90 AS DECIMAL(10,2))),
  (202, DATE'2024-01-03', CAST(89.99  AS DECIMAL(10,2))),
  (202, DATE'2024-01-10', CAST(79.99  AS DECIMAL(10,2))),
  (202, DATE'2024-01-25', CAST(84.99  AS DECIMAL(10,2))),
  (302, DATE'2024-01-05', CAST(159.00 AS DECIMAL(10,2))),
  (302, DATE'2024-01-31', CAST(149.00 AS DECIMAL(10,2))),
  (401, DATE'2024-01-01', CAST(24.90  AS DECIMAL(10,2)))
AS price_history(product_id, price_date, price);


-- =====================================================================================
-- Q13 — Gaps and islands
-- =====================================================================================
-- Q13a — Stock-state islands per product/warehouse (IN_STOCK / STOCKOUT / BACKORDER)
-- @id: Q13a
WITH mv AS (
  SELECT
    *,
    CASE WHEN move_type IN ('RECEIPT', 'ADJUST_IN') THEN qty ELSE -qty END AS signed_qty
  FROM inventory_movements
),
bal AS (
  SELECT
    *,
    SUM(signed_qty) OVER (
      PARTITION BY product_id, warehouse ORDER BY move_ts, move_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS on_hand,
    LEAD(move_ts) OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id) AS next_move_ts
  FROM mv
),
stated AS (
  SELECT
    *,
    CASE WHEN on_hand > 0 THEN 'IN_STOCK' WHEN on_hand = 0 THEN 'STOCKOUT' ELSE 'BACKORDER' END AS stock_state
  FROM bal
),
keyed AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id)
  - ROW_NUMBER() OVER (PARTITION BY product_id, warehouse, stock_state ORDER BY move_ts, move_id) AS island_key
  FROM stated
),
islands AS (
  SELECT
    product_id,
    warehouse,
    stock_state,
    island_key,
    MIN(move_ts)                                                         AS state_from,
    -- the island is still open if its last row has no next move
    CASE WHEN bool_or(next_move_ts IS NULL) THEN NULL ELSE MAX(next_move_ts) END AS state_to,
    COUNT(*)                                                             AS moves,
    MIN(on_hand)                                                         AS min_on_hand,
    MAX(on_hand)                                                         AS max_on_hand,
    SUM(signed_qty)                                                      AS net_change,
    max_by(move_type, struct(move_ts, move_id))                          AS last_move_type
  FROM keyed
  GROUP BY product_id, warehouse, stock_state, island_key
)
SELECT
  product_id,
  warehouse,
  ROW_NUMBER() OVER (PARTITION BY product_id, warehouse ORDER BY state_from)            AS island_no,
  stock_state,
  state_from,
  state_to,
  state_to IS NULL                                                                       AS is_current,
  round(timestampdiff(MINUTE, state_from, coalesce(state_to, TIMESTAMP'2024-08-01 00:00:00')) / 1440.0, 2) AS duration_days,
  moves, min_on_hand, max_on_hand, net_change, last_move_type,
  SUM(CASE WHEN stock_state <> 'IN_STOCK'
           THEN timestampdiff(MINUTE, state_from, coalesce(state_to, TIMESTAMP'2024-08-01 00:00:00')) END)
    OVER (PARTITION BY product_id, warehouse) / 1440.0                                   AS total_unavailable_days,
  LAG(stock_state, 1, 'NEW') OVER (PARTITION BY product_id, warehouse ORDER BY state_from) AS prev_state
FROM islands
ORDER BY product_id, warehouse, state_from;

-- Q13b — Consecutive-day activity streaks per customer (web OR order OR ticket)
-- @id: Q13b
WITH activity AS (
  SELECT customer_id, to_date(event_ts)  AS d, 'web'    AS src FROM web_events      WHERE customer_id IS NOT NULL
  UNION ALL
  SELECT customer_id, to_date(order_ts)  AS d, 'order'  AS src FROM orders
  UNION ALL
  SELECT customer_id, to_date(opened_ts) AS d, 'ticket' AS src FROM support_tickets WHERE customer_id IS NOT NULL
),
days AS (
  SELECT customer_id, d, array_sort(collect_set(src)) AS sources
  FROM activity
  GROUP BY customer_id, d
),
grouped AS (
  SELECT
    *,
    date_sub(d, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY d)) AS streak_anchor
  FROM days
),
streaks AS (
  SELECT
    customer_id,
    streak_anchor,
    MIN(d)                                     AS streak_start,
    MAX(d)                                     AS streak_end,
    datediff(MAX(d), MIN(d)) + 1               AS streak_days,
    array_distinct(flatten(collect_list(sources))) AS streak_sources
  FROM grouped
  GROUP BY customer_id, streak_anchor
),
with_gaps AS (
  SELECT
    *,
    datediff(streak_start, LAG(streak_end) OVER (PARTITION BY customer_id ORDER BY streak_start)) - 1 AS idle_days_before
  FROM streaks
)
SELECT
  c.customer_id,
  c.full_name,
  COUNT(w.streak_start)                                                       AS n_streaks,
  coalesce(SUM(w.streak_days), 0)                                             AS active_days,
  MAX(w.streak_days)                                                          AS longest_streak_days,
  -- tie-break: earliest start wins (negative date via datediff for ordering)
  max_by(w.streak_start, struct(w.streak_days, -datediff(w.streak_start, DATE'2000-01-01'))) AS longest_streak_start,
  MAX(w.idle_days_before)                                                     AS longest_idle_gap_days,
  array_sort(array_distinct(flatten(collect_list(w.streak_sources))))         AS all_sources,
  transform(
    array_sort(collect_list(struct(w.streak_start, w.streak_days))),
    s -> concat(date_format(s.streak_start, 'MM-dd'), 'x', s.streak_days)
  )                                                                           AS streak_list,
  count_if(w.streak_days >= 2)                                                AS multi_day_streaks
FROM customers c
LEFT JOIN with_gaps w ON w.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name
ORDER BY c.customer_id;


-- =====================================================================================
-- Q14 — FIFO inventory costing via cumulative-range overlap
-- =====================================================================================
-- Q14a — COGS per outbound movement
-- @id: Q14a
WITH layers AS (
  SELECT
    product_id, warehouse, move_id AS in_id, move_ts AS in_ts, qty AS in_qty, unit_cost,
    SUM(qty) OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - qty AS cum_in_start,
    SUM(qty) OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS cum_in_end
  FROM inventory_movements
  WHERE move_type IN ('RECEIPT', 'ADJUST_IN')
),
consumption AS (
  SELECT
    product_id, warehouse, move_id AS out_id, move_ts AS out_ts, move_type, qty AS out_qty,
    SUM(qty) OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - qty AS cum_out_start,
    SUM(qty) OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS cum_out_end
  FROM inventory_movements
  WHERE move_type IN ('SALE', 'ADJUST_OUT')
),
matched AS (
  SELECT
    c.*,
    l.in_id, l.in_ts, l.unit_cost,
    -- least()/greatest() SKIP NULLs: without the CASE an unmatched LEFT JOIN row would
    -- yield least(x, NULL) - greatest(y, NULL) = x - y instead of NULL
    CASE WHEN l.in_id IS NOT NULL
         THEN least(c.cum_out_end, l.cum_in_end) - greatest(c.cum_out_start, l.cum_in_start) END AS take_qty
  FROM consumption c
  LEFT JOIN layers l
    ON  l.product_id   = c.product_id
    AND l.warehouse    = c.warehouse
    AND l.cum_in_start < c.cum_out_end
    AND l.cum_in_end   > c.cum_out_start
),
per_out AS (
  SELECT
    out_id, product_id, warehouse, out_ts, move_type, out_qty,
    coalesce(SUM(take_qty), 0)                                            AS matched_qty,
    CAST(SUM(take_qty * unit_cost) AS DECIMAL(14,2))                      AS cogs_usd,
    COUNT(in_id)                                                          AS n_layers,
    filter(
      array_sort(collect_list(named_struct('in_id', in_id, 'qty', take_qty, 'unit_cost', unit_cost))),
      x -> x.in_id IS NOT NULL
    )                                                                     AS layers_used,
    coalesce(bool_or(in_ts > out_ts), false)                              AS filled_from_future_receipt
  FROM matched
  GROUP BY out_id, product_id, warehouse, out_ts, move_type, out_qty
)
SELECT
  p.out_id,
  p.product_id,
  pr.product_name,
  p.warehouse,
  p.out_ts,
  p.move_type,
  p.out_qty,
  p.matched_qty,
  p.out_qty - p.matched_qty                                               AS unmatched_qty,
  p.cogs_usd,
  CAST(try_divide(p.cogs_usd, p.matched_qty) AS DECIMAL(12,4))            AS avg_unit_cost,
  p.n_layers,
  p.layers_used,
  p.filled_from_future_receipt,
  SUM(p.cogs_usd) OVER (PARTITION BY p.product_id ORDER BY p.out_ts, p.out_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS product_running_cogs,
  round(p.cogs_usd / nullif(SUM(p.cogs_usd) OVER (PARTITION BY p.warehouse), 0) * 100, 2) AS pct_of_wh_cogs
FROM per_out p
JOIN products pr ON pr.product_id = p.product_id
ORDER BY p.product_id, p.warehouse, p.out_ts, p.out_id;

-- Q14b — Ending inventory valuation (remaining FIFO layers) with ROLLUP
-- @id: Q14b
WITH layers AS (
  SELECT
    product_id, warehouse, move_id AS in_id, move_ts AS in_ts, qty AS in_qty, unit_cost,
    SUM(qty) OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_in_end
  FROM inventory_movements
  WHERE move_type IN ('RECEIPT', 'ADJUST_IN')
),
total_out AS (
  SELECT product_id, warehouse, SUM(qty) AS out_qty
  FROM inventory_movements
  WHERE move_type IN ('SALE', 'ADJUST_OUT')
  GROUP BY product_id, warehouse
),
remaining AS (
  SELECT
    l.*,
    -- units of this layer still on the shelf = overlap of [cum_in_start, cum_in_end) with [out_qty, +inf)
    greatest(0, l.cum_in_end - greatest(l.cum_in_end - l.in_qty, coalesce(t.out_qty, 0))) AS remaining_qty
  FROM layers l
  LEFT JOIN total_out t ON t.product_id = l.product_id AND t.warehouse = l.warehouse
)
SELECT
  coalesce(r.warehouse, '(all warehouses)')                                AS warehouse,
  CASE WHEN grouping(r.product_id) = 1 THEN '(all products)'
       ELSE CAST(r.product_id AS STRING) END                               AS product,
  grouping_id(r.warehouse, r.product_id)                                   AS gid,
  SUM(r.remaining_qty)                                                     AS units_on_hand,
  CAST(SUM(r.remaining_qty * r.unit_cost) AS DECIMAL(14,2))                AS fifo_value_usd,
  CAST(try_divide(SUM(r.remaining_qty * r.unit_cost), SUM(r.remaining_qty)) AS DECIMAL(12,4)) AS wavg_unit_cost,
  count_if(r.remaining_qty > 0)                                            AS open_layers,
  datediff(DATE'2024-07-31', to_date(MIN(r.in_ts) FILTER (WHERE r.remaining_qty > 0))) AS oldest_layer_age_days,
  MAX(r.unit_cost) FILTER (WHERE r.remaining_qty > 0)
    - MIN(r.unit_cost) FILTER (WHERE r.remaining_qty > 0)                  AS cost_spread
FROM remaining r
GROUP BY ROLLUP (r.warehouse, r.product_id)
ORDER BY gid, warehouse, product;


-- =====================================================================================
-- Q15 — Slowly changing dimension (Type 2) and as-of joins
-- =====================================================================================
-- Q15a — Tier in force at the time of each order
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

-- Q15b — Tier timeline per customer: days per tier, upgrades/downgrades, drift check
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


-- =====================================================================================
-- Q16 — Hierarchy flattening (fixed-depth self-joins) and revenue roll-up
-- =====================================================================================
-- @id: Q16
WITH walk AS (
  SELECT
    n.node_id, n.parent_id, n.node_name,
    p1.node_id AS a1_id, p1.node_name AS a1_name,
    p2.node_id AS a2_id, p2.node_name AS a2_name,
    p3.node_id AS a3_id, p3.node_name AS a3_name
  FROM category_tree n
  LEFT JOIN category_tree p1 ON p1.node_id = n.parent_id
  LEFT JOIN category_tree p2 ON p2.node_id = p1.parent_id
  LEFT JOIN category_tree p3 ON p3.node_id = p2.parent_id
),
paths AS (
  SELECT
    node_id, parent_id, node_name,
    filter(array(a3_id, a2_id, a1_id, node_id), x -> x IS NOT NULL)                AS path_ids,
    filter(array(a3_name, a2_name, a1_name, node_name), x -> x IS NOT NULL)        AS path_names,
    parent_id IS NOT NULL AND a1_id IS NULL                                        AS is_orphan
  FROM walk
),
nodes AS (
  SELECT
    p.*,
    size(p.path_ids) - 1                                                           AS depth,
    NOT EXISTS (SELECT 1 FROM category_tree ch WHERE ch.parent_id = p.node_id)     AS is_leaf
  FROM paths p
),
product_node AS (
  SELECT
    pr.product_id,
    coalesce(n.node_id, 40) AS node_id            -- unmapped products go to Clearance
  FROM products pr
  LEFT JOIN nodes n
    ON  n.is_leaf
    AND n.node_name = pr.sub_category
    -- try_element_at: the orphan node's path has one element, element_at(.., -2) would throw
    AND lower(try_element_at(n.path_names, -2)) = pr.category
),
fx AS (
  SELECT order_id, rate_to_usd FROM (
    SELECT o.order_id, f.rate_to_usd,
           ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY f.rate_date DESC) AS rn
    FROM orders o JOIN fx_rates f ON f.currency = o.currency AND f.rate_date <= to_date(o.order_ts)
  ) WHERE rn = 1
),
line_usd AS (
  SELECT oi.order_id, oi.product_id, oi.quantity,
         oi.quantity * oi.unit_price * (1 - oi.discount_pct) * fx.rate_to_usd AS net_usd
  FROM order_items oi
  JOIN orders o ON o.order_id = oi.order_id AND o.status IN ('DELIVERED', 'SHIPPED')
  JOIN fx       ON fx.order_id = oi.order_id
),
node_rev AS (
  SELECT
    anc_id                                       AS node_id,
    round(SUM(l.net_usd), 2)                     AS rev_usd,
    SUM(l.quantity)                              AS units,
    COUNT(DISTINCT l.order_id)                   AS orders,
    array_sort(collect_set(l.product_id))        AS products
  FROM line_usd l
  JOIN product_node pn ON pn.product_id = l.product_id
  JOIN nodes n         ON n.node_id     = pn.node_id
  LATERAL VIEW explode(n.path_ids) e AS anc_id
  GROUP BY anc_id
),
leaf_counts AS (
  SELECT anc_id AS node_id, COUNT(*) AS leaves_below
  FROM nodes n
  LATERAL VIEW explode(n.path_ids) e AS anc_id
  WHERE n.is_leaf AND n.node_id <> anc_id
  GROUP BY anc_id
)
SELECT
  n.node_id,
  n.parent_id,
  concat(repeat('  ', n.depth), n.node_name)                                     AS tree_label,
  concat_ws(' > ', n.path_names)                                                 AS full_path,
  n.depth,
  n.is_leaf,
  n.is_orphan,
  element_at(n.path_names, 1)                                                    AS root_name,
  coalesce(lc.leaves_below, 0)                                                   AS leaves_below,
  coalesce(r.rev_usd, 0)                                                         AS rev_usd,
  coalesce(r.units, 0)                                                           AS units,
  coalesce(r.orders, 0)                                                          AS orders,
  coalesce(r.products, array())                                                  AS products,
  round(100 * coalesce(r.rev_usd, 0) / nullif(pr.rev_usd, 0), 1)                 AS pct_of_parent,
  round(100 * coalesce(r.rev_usd, 0)
        / nullif(MAX(CASE WHEN n.depth = 0 AND NOT n.is_orphan THEN r.rev_usd END) OVER (), 0), 1) AS pct_of_total,
  DENSE_RANK() OVER (PARTITION BY n.parent_id ORDER BY coalesce(r.rev_usd, 0) DESC) AS sibling_rank,
  COUNT(*) OVER (PARTITION BY n.parent_id)                                       AS siblings
FROM nodes n
LEFT JOIN node_rev    r  ON r.node_id  = n.node_id
LEFT JOIN node_rev    pr ON pr.node_id = n.parent_id
LEFT JOIN leaf_counts lc ON lc.node_id = n.node_id
ORDER BY full_path;


-- =====================================================================================
-- Q17 — Free-text mining on support tickets
-- =====================================================================================
-- Q17a — One row per ticket, every field cleaned / extracted
-- @id: Q17a
WITH t AS (
  SELECT
    *,
    nullif(lower(trim(agent_email)), '')                                       AS agent_email_clean,
    str_to_map(meta, ';', '=')                                                 AS meta_map
  FROM support_tickets
),
parsed AS (
  SELECT
    t.*,
    initcap(translate(substring_index(agent_email_clean, '@', 1), '._', '  ')) AS agent_name,
    regexp_replace(subject, '(?i)^((re|fw|fwd):\\s*)+', '')                    AS subject_core,
    length(subject) - length(replace(subject, '!', ''))                       AS exclamations,
    array_distinct(transform(
      regexp_extract_all(body, '(?i)(?:ord-|order\\s*#?\\s*)(\\d{4})', 1),
      x -> CAST(x AS INT)))                                                    AS order_refs,
    regexp_extract(body, '(\\+\\d{2}-\\d{2}-\\d{3}-\\d{4})', 1)                 AS phone_raw,
    regexp_extract(body, '([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,})', 1) AS email_in_body,
    regexp_extract(body, '\\*{4}(\\d{4})', 1)                                   AS card_last4,
    split(tags, ';')                                                           AS tag_arr,
    element_at(meta_map, 'lang')                                               AS lang,
    coalesce(CAST(element_at(meta_map, 'vip') AS INT), 0) = 1                  AS is_vip,
    parse_url(element_at(meta_map, 'utm'), 'QUERY', 'utm_source')             AS utm_source
  FROM t
)
SELECT
  ticket_id,
  customer_id,
  left(sha2(concat_ws('|', ticket_id, coalesce(CAST(customer_id AS STRING), 'anon')), 256), 16) AS anon_key,
  agent_email_clean,
  agent_name,
  subject_core,
  upper(subject_core) = subject_core OR subject RLIKE '\\b[A-Z]{4,}\\b'       AS is_shouting,
  exclamations,
  order_refs,
  size(order_refs)                                                           AS n_order_refs,
  -- look-ahead: mask every digit that still has 4+ digits after it
  -- regexp_extract returns '' (not NULL) when nothing matches
  regexp_replace(nullif(phone_raw, ''), '\\d(?=.*\\d{4}$)', '#')            AS phone_masked,
  nullif(email_in_body, '')                                                  AS email_in_body,
  regexp_replace(body, '\\*{4}(\\d{4})', 'card-x$1')                         AS body_redacted,
  nullif(card_last4, '')                                                     AS card_last4,
  tag_arr,
  array_contains(tag_arr, 'defect')                                          AS is_defect,
  lang,
  is_vip,
  utm_source,
  length(body)                                                               AS body_chars,
  octet_length(body)                                                         AS body_bytes,
  body RLIKE '[\\x{0E00}-\\x{0E7F}]'                                          AS has_thai,
  size(filter(split(lower(body), '[^a-z]+'), w -> w IN ('please', 'thanks', 'thank'))) AS politeness_words,
  levenshtein(lower(subject_core), 'where is my order?') <= 3                 AS is_wismo,
  CASE
    WHEN lower(body) LIKE ANY ('%refund%', '%return%')         THEN 'returns'
    WHEN body ILIKE '%cancel%'                                 THEN 'cancellation'
    WHEN lower(body) RLIKE 'payment|card|wallet'               THEN 'payments'
    WHEN tags LIKE '%presales%'                                THEN 'presales'
    WHEN subject LIKE 'RE!_%' ESCAPE '!' OR body = 'unsubscribe' THEN 'noise'
    ELSE 'product'
  END                                                                        AS topic,
  format_string('%s|%-5s|%3d chars', priority, channel, length(body))         AS summary_line,
  lpad(CAST(coalesce(csat, 0) AS STRING), 3, '0')                           AS csat_padded,
  overlay(ticket_id PLACING 'TKT' FROM 1 FOR 1)                               AS ticket_alt_id
FROM parsed
ORDER BY ticket_id;

-- Q17b — Tag analytics: explode, positional tags, per-tag SLA and satisfaction
-- @id: Q17b
WITH sla AS (
  SELECT
    t.*,
    map('P1', 60, 'P2', 240, 'P3', 1440, 'P4', 2880)[priority]               AS sla_minutes,
    timestampdiff(MINUTE, opened_ts, first_response_ts)                       AS response_minutes
  FROM support_tickets t
)
SELECT
  tg.tag,
  COUNT(*)                                                                    AS tickets,
  count_if(tg.pos = 0)                                                        AS times_primary_tag,
  array_sort(collect_set(nullif(lower(trim(s.agent_email)), '')))             AS agents,
  round(avg(s.csat), 2)                                                       AS avg_csat,
  COUNT(s.csat)                                                               AS csat_responses,
  count_if(s.response_minutes IS NULL OR s.response_minutes > s.sla_minutes)  AS sla_breaches,
  percentile(s.response_minutes, 0.5)                                         AS median_response_min,
  MAX(s.response_minutes)                                                     AS worst_response_min,
  concat_ws(',', array_sort(collect_list(s.ticket_id)))                       AS ticket_ids,
  min(s.priority)                                                             AS top_priority
FROM sla s
LATERAL VIEW posexplode(split(s.tags, ';')) tg AS pos, tag
GROUP BY tg.tag
HAVING COUNT(*) >= 1
ORDER BY tickets DESC, tg.tag;


-- =====================================================================================
-- Q18 — Time bucketing, session windows and business-hours SLA
-- =====================================================================================
-- Q18a — Weekly (Monday-aligned) tumbling + 30-day sliding windows over orders
-- @id: Q18a
WITH ord AS (
  SELECT o.order_id, o.order_ts, o.channel, o.status,
         SUM(oi.quantity) AS units
  FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
  GROUP BY o.order_id, o.order_ts, o.channel, o.status
),
weekly AS (
  SELECT
    'weekly' AS grain,
    -- epoch (1970-01-01) is a Thursday, so a 4-day startTime aligns buckets to Monday
    window(order_ts, '1 week', '1 week', '4 days') AS w,
    COUNT(*) AS orders, SUM(units) AS units,
    array_sort(collect_set(channel)) AS channels
  FROM ord
  GROUP BY window(order_ts, '1 week', '1 week', '4 days')
),
sliding AS (
  SELECT
    'sliding_30d_step_15d' AS grain,
    window(order_ts, '30 days', '15 days') AS w,
    COUNT(*) AS orders, SUM(units) AS units,
    array_sort(collect_set(channel)) AS channels
  FROM ord
  WHERE status <> 'CANCELLED'
  GROUP BY window(order_ts, '30 days', '15 days')
)
SELECT
  grain,
  w.start                                AS bucket_start,
  w.end                                  AS bucket_end,
  date_format(w.start, 'EEE')            AS start_weekday,
  orders,
  units,
  channels,
  SUM(orders) OVER (PARTITION BY grain ORDER BY w.start
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_orders
FROM (SELECT * FROM weekly UNION ALL SELECT * FROM sliding)
ORDER BY grain, bucket_start;

-- Q18b — Session windows with a dynamic gap (checkout closes a session faster)
-- @id: Q18b
SELECT
  customer_id,
  session_window.start                                              AS session_start,
  session_window.end                                                AS session_end,
  timestampdiff(MINUTE, session_window.start, session_window.end)  AS window_minutes,
  COUNT(*)                                                          AS events,
  max(event_type = 'checkout')                                      AS converted,
  min_by(event_type, event_ts)                                      AS first_event,
  max_by(event_type, event_ts)                                      AS last_event,
  date_format(from_utc_timestamp(session_window.start, 'Asia/Bangkok'), 'yyyy-MM-dd HH:mm') AS start_bkk
FROM web_events
WHERE customer_id IS NOT NULL
GROUP BY customer_id,
         session_window(event_ts, CASE WHEN event_type = 'checkout' THEN '5 minutes' ELSE '20 minutes' END)
ORDER BY customer_id, session_start;

-- Q18c — Ticket SLA in Bangkok local business hours (Mon-Fri 09:00-18:00)
-- @id: Q18c
WITH local AS (
  SELECT
    ticket_id, priority, channel,
    from_utc_timestamp(opened_ts, 'Asia/Bangkok')                                  AS opened_bkk,
    from_utc_timestamp(coalesce(first_response_ts, resolved_ts, TIMESTAMP'2024-08-01 00:00:00'), 'Asia/Bangkok') AS responded_bkk,
    first_response_ts IS NULL                                                       AS no_response
  FROM support_tickets
),
hours AS (
  SELECT
    l.*,
    sequence(date_trunc('HOUR', opened_bkk), date_trunc('HOUR', responded_bkk), INTERVAL 1 HOUR) AS hour_starts
  FROM local l
),
biz AS (
  SELECT
    h.*,
    aggregate(
      filter(hour_starts, hs -> dayofweek(hs) BETWEEN 2 AND 6 AND hour(hs) BETWEEN 9 AND 17),
      0L,
      (acc, hs) -> acc + greatest(0L,
                     timestampdiff(MINUTE,
                                   greatest(hs, opened_bkk),
                                   least(hs + INTERVAL 1 HOUR, responded_bkk)))
    )                                                                               AS business_minutes
  FROM hours h
)
SELECT
  ticket_id,
  priority,
  date_format(opened_bkk, 'EEE yyyy-MM-dd HH:mm')                                  AS opened_local,
  dayofweek(opened_bkk) IN (1, 7)                                                  AS opened_on_weekend,
  hour(opened_bkk) NOT BETWEEN 9 AND 17                                            AS opened_after_hours,
  timestampdiff(MINUTE, opened_bkk, responded_bkk)                                 AS wall_clock_minutes,
  business_minutes,
  no_response,
  CASE priority WHEN 'P1' THEN 60 WHEN 'P2' THEN 240 WHEN 'P3' THEN 540 ELSE 1080 END AS sla_business_minutes,
  business_minutes > CASE priority WHEN 'P1' THEN 60 WHEN 'P2' THEN 240 WHEN 'P3' THEN 540 ELSE 1080 END AS sla_breached,
  CASE WHEN dayofweek(opened_bkk) IN (1, 7) THEN next_day(to_date(opened_bkk), 'MON')
       ELSE to_date(opened_bkk) END                                                AS first_business_day,
  last_day(opened_bkk)                                                             AS month_end,
  add_months(trunc(opened_bkk, 'QUARTER'), 3)                                      AS next_quarter_start,
  weekofyear(opened_bkk)                                                           AS iso_week,
  extract(DOY FROM opened_bkk)                                                     AS day_of_year,
  make_timestamp(year(opened_bkk), month(opened_bkk), day(opened_bkk), 18, 0, 0)  AS same_day_cutoff,
  size(hour_starts)                                                                AS hours_spanned
FROM biz
ORDER BY ticket_id;


-- =====================================================================================
-- Q19 — Time-series gap filling and interpolation
-- =====================================================================================
-- @id: Q19
WITH spine AS (
  SELECT p.product_id, d
  FROM (SELECT DISTINCT product_id FROM price_history) p
  LATERAL VIEW explode(sequence(DATE'2024-01-01', DATE'2024-01-31', INTERVAL 1 DAY)) s AS d
),
joined AS (
  SELECT s.product_id, s.d, ph.price
  FROM spine s
  LEFT JOIN price_history ph ON ph.product_id = s.product_id AND ph.price_date = s.d
),
filled AS (
  SELECT
    *,
    -- two spellings of IGNORE NULLS on purpose
    last_value(price, true) OVER w_past                                            AS ffill,
    FIRST_VALUE(price) IGNORE NULLS OVER w_future                                  AS bfill,
    last_value(CASE WHEN price IS NOT NULL THEN d END, true) OVER w_past           AS prev_known_d,
    first_value(CASE WHEN price IS NOT NULL THEN d END, true) OVER w_future        AS next_known_d
  FROM joined
  WINDOW
    w_past   AS (PARTITION BY product_id ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w_future AS (PARTITION BY product_id ORDER BY d ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
),
interp AS (
  SELECT
    *,
    CASE
      WHEN price IS NOT NULL                          THEN CAST(price AS DECIMAL(12,4))
      WHEN prev_known_d IS NULL OR next_known_d IS NULL THEN NULL
      ELSE CAST(ffill + (bfill - ffill) * datediff(d, prev_known_d) / datediff(next_known_d, prev_known_d)
                AS DECIMAL(12,4))
    END AS interp_price
  FROM filled
),
final AS (
  SELECT
    *,
    CAST(coalesce(interp_price, ffill, bfill) AS DECIMAL(12,4))                      AS price_filled,
    CASE
      WHEN price IS NOT NULL       THEN 'observed'
      WHEN interp_price IS NOT NULL THEN 'interpolated'
      WHEN ffill IS NOT NULL       THEN 'forward_filled'
      WHEN bfill IS NOT NULL       THEN 'back_filled'
      ELSE 'missing'
    END                                                                              AS fill_method
  FROM interp
)
SELECT
  product_id,
  d,
  price,
  ffill,
  bfill,
  interp_price,
  price_filled,
  fill_method,
  -- dense spine: ROWS and RANGE agree here...
  round(AVG(price_filled) OVER (PARTITION BY product_id ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 4) AS ma7_rows,
  round(AVG(price_filled) OVER (PARTITION BY product_id ORDER BY d
                                RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW), 4)                  AS ma7_range,
  -- ...but on the raw sparse observations they don't (NULL prices are ignored by AVG, not by the frame)
  round(AVG(price) OVER (PARTITION BY product_id ORDER BY d
                         RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW), 4)                         AS ma7_observed_only,
  price_filled - LAG(price_filled, 7) OVER (PARTITION BY product_id ORDER BY d)                               AS wow_change,
  round(100 * (price_filled / LAG(price_filled) OVER (PARTITION BY product_id ORDER BY d) - 1), 3)            AS dod_pct,
  datediff(d, prev_known_d)                                                                                   AS days_since_observation,
  MIN(price_filled) OVER (PARTITION BY product_id)                                                            AS month_low,
  -- struct ordering breaks ties (401 has a flat price all month): earliest low day wins
  d = min_by(d, struct(price_filled, d)) OVER (PARTITION BY product_id)                                      AS is_low_day
FROM final
ORDER BY product_id, d;


-- =====================================================================================
-- Q20 — Statistical and ordered-set aggregates
-- =====================================================================================
-- Q20a — Distribution stats per category x channel (GROUP BY ALL)
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

-- Q20b — Histogram with width_bucket + window over an aggregate in the same SELECT
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


-- =====================================================================================
-- Q21 — Arrays and maps deep-dive
-- =====================================================================================
-- Q21a — Per-customer order history as arrays, ~30 array/map functions in one SELECT
-- @id: Q21a
WITH hist AS (
  SELECT
    c.customer_id, c.interests, c.preferences,
    array_sort(collect_list(
      CASE WHEN o.order_id IS NOT NULL THEN
        named_struct('ts', o.order_ts, 'id', o.order_id, 'status', o.status, 'channel', o.channel) END
    )) AS orders_arr
  FROM customers c
  LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.interests, c.preferences
),
x AS (
  SELECT
    h.*,
    transform(orders_arr, o -> o.id)      AS ids,
    transform(orders_arr, o -> o.status)  AS statuses,
    transform(orders_arr, o -> o.channel) AS channels
  FROM hist h
)
SELECT
  customer_id,
  ids,
  slice(ids, 1, 2)                                                         AS first_two,
  slice(ids, -2, 2)                                                        AS last_two,
  CASE WHEN size(ids) > 0 THEN element_at(ids, -1) END                     AS last_id,   -- guard: throws on [] under ANSI
  try_element_at(ids, 5)                                                   AS fifth_id_or_null,
  array_position(statuses, 'DELIVERED')                                    AS first_delivered_pos,
  arrays_zip(ids, statuses)                                                AS id_status_pairs,
  array_remove(statuses, 'DELIVERED')                                      AS non_delivered,
  array_union(interests, array('electronics'))                             AS interests_plus_elec,
  array_intersect(array_distinct(channels), array('app', 'market'))        AS non_web_channels,
  sort_array(array_distinct(channels), false)                              AS channels_desc,
  array_join(array_repeat('*', size(ids)), '')                             AS stars,
  transform(ids, (id, i) -> concat('#', i + 1, '=', id))                   AS numbered,
  filter(ids, (id, i) -> i % 2 = 0)                                        AS odd_positions,
  reduce(ids, 0L, (acc, id) -> acc + id, acc -> CASE WHEN size(ids) = 0 THEN NULL ELSE acc / size(ids) END) AS avg_id,
  aggregate(
    statuses,
    named_struct('switches', 0, 'prev', CAST(NULL AS STRING)),
    (acc, s) -> named_struct(
                  'switches', acc.switches + CASE WHEN acc.prev IS NOT NULL AND acc.prev <> s THEN 1 ELSE 0 END,
                  'prev', s),
    acc -> acc.switches
  )                                                                        AS status_switches,
  exists(statuses, s -> s = 'RETURNED')                                    AS ever_returned,
  -- three-valued logic: array has a NULL and no match -> NULL, not false
  exists(array_append(statuses, CAST(NULL AS STRING)), s -> s = 'LOST')    AS ever_lost_3vl,
  array_compact(array_append(channels, CAST(NULL AS STRING)))              AS channels_compacted,
  array_prepend(ids, 0)                                                    AS ids_with_sentinel,
  array_insert(interests, 1, 'all')                                        AS interests_with_all,
  map_from_arrays(transform(ids, id -> CAST(id AS STRING)), statuses)      AS status_by_order,
  map_concat(preferences, map('orders', CAST(size(ids) AS STRING)))        AS prefs_plus_count,
  map_zip_with(
    preferences, map('newsletter', 'Y', 'sms', 'Y', 'push', 'N'),
    (k, mine, dflt) -> CASE WHEN mine IS NULL THEN concat('default:', dflt)
                            WHEN dflt IS NULL THEN concat('custom:', mine)
                            WHEN mine = dflt  THEN 'same'
                            ELSE concat(dflt, '->', mine) END
  )                                                                        AS prefs_vs_default,
  transform_keys(preferences, (k, v) -> upper(k))                          AS prefs_upper,
  map_entries(map_filter(preferences, (k, v) -> v = 'N'))                  AS opted_out_entries,
  sequence(1, greatest(size(ids), 1))                                      AS seq_to_n
FROM x
ORDER BY customer_id;

-- Q21b — Generators over maps and structs: explode(map), posexplode_outer, inline_outer
-- @id: Q21b
SELECT
  c.customer_id,
  pref.pref_key,
  pref.pref_val,
  intr.interest_pos,
  intr.interest,
  hist.kind,
  hist.val
FROM customers c
LATERAL VIEW OUTER explode(c.preferences)                       pref AS pref_key, pref_val
LATERAL VIEW OUTER posexplode(c.interests)                      intr AS interest_pos, interest
LATERAL VIEW OUTER inline(
  CASE WHEN c.tier IS NULL THEN array()
       ELSE array(named_struct('kind', 'tier', 'val', c.tier),
                  named_struct('kind', 'city', 'val', c.city)) END
)                                                               hist AS kind, val
WHERE c.customer_id IN (1, 4, 5, 10)
ORDER BY c.customer_id, pref.pref_key, intr.interest_pos, hist.kind;


-- =====================================================================================
-- Q22 — Advanced subqueries
-- =====================================================================================
-- Q22a — Chained correlated LATERAL subqueries, EXISTS in SELECT / CASE, nested CTE
-- @id: Q22a
SELECT
  c.customer_id,
  c.full_name,
  agg.n_orders,
  agg.last_order_ts,
  agg.n_channels,
  derived.orders_per_year,
  EXISTS (SELECT 1 FROM support_tickets t WHERE t.customer_id = c.customer_id)          AS has_ticket,
  CASE
    WHEN EXISTS (SELECT 1 FROM support_tickets t
                  WHERE t.customer_id = c.customer_id AND t.priority = 'P1')            THEN 'escalated'
    WHEN NOT EXISTS (SELECT 1 FROM web_events w WHERE w.customer_id = c.customer_id)     THEN 'offline'
    ELSE 'normal'
  END                                                                                   AS care_flag,
  top_cat.category                                                                      AS top_category
FROM customers c,
LATERAL (
  SELECT COUNT(*) AS n_orders, MAX(o.order_ts) AS last_order_ts, COUNT(DISTINCT o.channel) AS n_channels
  FROM orders o
  WHERE o.customer_id = c.customer_id
) agg,
LATERAL (
  SELECT round(agg.n_orders / greatest(months_between(DATE'2024-08-01', c.signup_date) / 12, 0.5), 3) AS orders_per_year
) derived
LEFT JOIN (
  -- CTE nested inside a subquery
  WITH cat_units AS (
    SELECT o.customer_id, p.category, SUM(oi.quantity) AS units
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    JOIN products p     ON p.product_id = oi.product_id
    GROUP BY o.customer_id, p.category
  )
  SELECT customer_id, max_by(category, struct(units, category)) AS category
  FROM cat_units
  GROUP BY customer_id
) top_cat ON top_cat.customer_id = c.customer_id
ORDER BY c.customer_id;

-- Q22b — Tuple IN subquery, correlated AVG comparison, scalar subquery in HAVING
-- @id: Q22b
WITH line_usd AS (
  SELECT oi.order_id, oi.line_no, oi.product_id, p.category, o.customer_id, o.order_ts,
         oi.unit_price * fx.rate_to_usd AS unit_usd
  FROM order_items oi
  JOIN orders o   ON o.order_id = oi.order_id
  JOIN products p ON p.product_id = oi.product_id
  JOIN (SELECT currency, max_by(rate_to_usd, rate_date) AS rate_to_usd FROM fx_rates GROUP BY currency) fx
    ON fx.currency = o.currency
)
SELECT
  l.customer_id,
  COUNT(*)                                          AS premium_lines,
  array_sort(collect_list(l.order_id))              AS premium_orders,
  round(SUM(l.unit_usd), 2)                         AS premium_unit_usd
FROM line_usd l
WHERE
  -- bought above the category's average unit price (correlated)
  l.unit_usd > (SELECT AVG(l2.unit_usd) FROM line_usd l2 WHERE l2.category = l.category)
  -- and the order came after a same-day checkout event (tuple IN)
  AND (l.customer_id, to_date(l.order_ts)) IN (
        SELECT customer_id, to_date(event_ts) FROM web_events WHERE event_type = 'checkout')
GROUP BY l.customer_id
HAVING COUNT(*) >= (SELECT MIN(cnt) FROM (SELECT COUNT(*) AS cnt FROM order_items GROUP BY order_id))
ORDER BY l.customer_id;

-- Q22c — Multiset set operations: INTERSECT ALL and EXCEPT ALL
-- @id: Q22c
WITH bought AS (            -- one row per unit-line, duplicates matter
  SELECT o.customer_id, p.category
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  JOIN products p     ON p.product_id = oi.product_id
  WHERE o.status <> 'CANCELLED'
),
wanted AS (               -- interests, doubled for tiers that get two recommendations
  SELECT c.customer_id, i AS category
  FROM customers c LATERAL VIEW explode(c.interests) e AS i
  UNION ALL
  SELECT c.customer_id, i
  FROM customers c LATERAL VIEW explode(c.interests) e AS i
  WHERE c.tier IN ('GOLD', 'PLATINUM')
)
SELECT 'bought_and_wanted' AS bucket, customer_id, category FROM (SELECT * FROM bought INTERSECT ALL SELECT * FROM wanted)
UNION ALL
SELECT 'wanted_not_bought' AS bucket, customer_id, category FROM (SELECT * FROM wanted EXCEPT ALL SELECT * FROM bought)
UNION ALL
SELECT 'bought_not_wanted' AS bucket, customer_id, category FROM (SELECT * FROM bought EXCEPT ALL SELECT * FROM wanted)
UNION ALL
SELECT 'distinct_overlap'  AS bucket, customer_id, category FROM (SELECT * FROM bought INTERSECT SELECT * FROM wanted)
ORDER BY bucket, customer_id, category;


-- =====================================================================================
-- Q23 — NULL semantics and numeric typing
-- =====================================================================================
-- Q23a — NOT IN against a subquery that yields a NULL returns NO rows; NOT EXISTS does
--         return rows. A converter that uses ~isin() or a left-anti join for NOT IN
--         will get the first branch wrong.
-- @id: Q23a
SELECT 'not_in' AS method, p.product_id, p.sub_category
FROM products p
WHERE p.sub_category NOT IN (SELECT sub_category FROM products WHERE category IN ('misc', 'books'))
UNION ALL
SELECT 'not_exists' AS method, p.product_id, p.sub_category
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM products q
                  WHERE q.category IN ('misc', 'books') AND q.sub_category = p.sub_category)
UNION ALL
SELECT 'in_with_null_list' AS method, p.product_id, p.sub_category
FROM products p
WHERE p.sub_category IN ('audio', NULL)           -- TRUE for audio, NULL (=filtered) otherwise
UNION ALL
SELECT 'not_in_with_null_list' AS method, p.product_id, p.sub_category
FROM products p
WHERE p.sub_category NOT IN ('audio', NULL)       -- never TRUE
ORDER BY method, product_id;

-- Q23b — Three-valued logic, null-aware aggregates and comparisons
-- @id: Q23b
SELECT
  country_code,
  COUNT(*)                                                    AS rows_all,
  COUNT(email)                                                AS rows_with_email,
  COUNT(DISTINCT tier)                                        AS distinct_tiers_non_null,
  count_if(tier = 'GOLD')                                     AS gold,
  count_if(NOT (tier = 'GOLD'))                               AS not_gold_excl_null,
  count_if(tier IS DISTINCT FROM 'GOLD')                      AS not_gold_incl_null,
  count_if(tier = NULL)                                       AS eq_null_always_0,
  SUM(CASE WHEN tier IS NULL THEN 1 END)                      AS null_tier_sum_or_null,
  concat(min(full_name), ' / ', max(email))                   AS concat_nullable,
  concat_ws(' / ', min(full_name), max(email))                AS concat_ws_skips_null,
  greatest(MIN(signup_date), CAST(NULL AS DATE))              AS greatest_ignores_null,
  nvl2(MAX(email), 'has email', 'no email')                   AS nvl2_check,
  nullif(COUNT(email), COUNT(*))                              AS missing_email_marker,
  sort_array(collect_list(email))                             AS emails_no_nulls,
  AVG(size(interests))                                        AS avg_interests,
  AVG(CASE WHEN size(interests) > 0 THEN size(interests) END) AS avg_interests_non_empty,
  bool_and(email LIKE '%@%')                                  AS all_emails_valid_3vl
FROM customers
GROUP BY country_code
ORDER BY country_code;

-- Q23c — Numeric typing: decimals, int division, rounding modes, try_* and ANSI casts
-- @id: Q23c
SELECT
  oi.order_id,
  oi.line_no,
  oi.unit_price,
  oi.quantity,
  oi.discount_pct,
  oi.unit_price / 3                                         AS dec_div,           -- decimal(..)
  typeof(oi.unit_price / 3)                                 AS t_dec_div,
  oi.quantity / 2                                           AS int_div_is_double,
  typeof(oi.quantity / 2)                                   AS t_int_div,
  oi.quantity div 2                                         AS int_div_floor,
  -oi.quantity % 3                                          AS neg_mod,
  pmod(-oi.quantity, 3)                                     AS pos_mod,
  round(oi.unit_price / 7, 2)                               AS round_half_up,
  bround(oi.unit_price / 8, 1)                              AS round_half_even,
  ceil(oi.unit_price / 7, 1)                                AS ceil_1dp,
  floor(-oi.unit_price / 7, -1)                             AS floor_tens,
  typeof(ceil(oi.unit_price / 7, 1))                        AS t_ceil,
  oi.quantity * oi.unit_price * (1 - oi.discount_pct)       AS net_exact,
  typeof(oi.quantity * oi.unit_price * (1 - oi.discount_pct)) AS t_net,
  CAST(oi.discount_pct AS FLOAT) + CAST(0.1 AS FLOAT)       AS float_sum,
  try_divide(oi.unit_price, oi.discount_pct)                AS price_per_disc,    -- NULL on /0
  try_multiply(2147483647, oi.quantity)                     AS int_mul_overflow,  -- NULL when > int max
  try_add(9223372036854775807L, CAST(oi.line_no - 1 AS BIGINT)) AS bigint_add_overflow,
  try_cast(concat(CAST(oi.quantity AS STRING), '.5') AS INT)  AS try_cast_frac,     -- NULL under ANSI
  CAST(concat(' ', CAST(oi.quantity AS STRING), ' ') AS INT)  AS cast_trims_spaces,
  CAST(oi.unit_price AS DECIMAL(10,0))                      AS narrowing_cast_rounds,
  format_number(oi.unit_price * 1000, 1)                    AS formatted,
  round(oi.unit_price, -2)                                  AS round_hundreds,
  shiftleft(oi.quantity, 3) | oi.line_no                    AS bit_mix,
  bit_count(oi.order_id)                                    AS bits_in_order_id
FROM order_items oi
ORDER BY oi.order_id, oi.line_no;


-- =====================================================================================
-- Q24 — Multi-column PIVOT, multi-value UNPIVOT, transpose
-- =====================================================================================
-- Q24a — PIVOT on (channel, status) tuples with two aggregates
-- @id: Q24a
SELECT *
FROM (
  -- only the columns PIVOT should group by may appear here (country_code)
  SELECT c.country_code, o.channel, o.status,
         oi.quantity * oi.unit_price * (1 - oi.discount_pct) AS net_local
  FROM orders o
  JOIN customers c    ON c.customer_id = o.customer_id
  JOIN order_items oi ON oi.order_id = o.order_id
) src
PIVOT (
  COUNT(*)                AS lines,
  round(SUM(net_local), 0) AS net
  FOR (channel, status) IN (
    ('web',    'DELIVERED') AS web_ok,
    ('app',    'DELIVERED') AS app_ok,
    ('market', 'DELIVERED') AS mkt_ok,
    ('web',    'CANCELLED') AS web_cxl,
    ('app',    'RETURNED')  AS app_ret,
    ('app',    'SHIPPED')   AS app_ship
  )
)
ORDER BY country_code;

-- Q24b — UNPIVOT two value columns at once, keeping NULLs
-- @id: Q24b
WITH wide AS (
  SELECT
    o.customer_id,
    COUNT(DISTINCT CASE WHEN quarter(o.order_ts) = 1 THEN o.order_id END)                 AS q1_orders,
    SUM(CASE WHEN quarter(o.order_ts) = 1 THEN oi.quantity END)                           AS q1_units,
    COUNT(DISTINCT CASE WHEN quarter(o.order_ts) = 2 THEN o.order_id END)                 AS q2_orders,
    SUM(CASE WHEN quarter(o.order_ts) = 2 THEN oi.quantity END)                           AS q2_units,
    COUNT(DISTINCT CASE WHEN quarter(o.order_ts) = 3 THEN o.order_id END)                 AS q3_orders,
    SUM(CASE WHEN quarter(o.order_ts) = 3 THEN oi.quantity END)                           AS q3_units
  FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
  GROUP BY o.customer_id
)
SELECT customer_id, quarter, n_orders, n_units,
       n_units IS NULL                                                   AS no_units,
       SUM(n_units) OVER (PARTITION BY customer_id ORDER BY quarter)    AS cum_units
FROM wide
UNPIVOT INCLUDE NULLS (
  (n_orders, n_units) FOR quarter IN (
    (q1_orders, q1_units) AS Q1,
    (q2_orders, q2_units) AS Q2,
    (q3_orders, q3_units) AS Q3
  )
)
ORDER BY customer_id, quarter;

-- Q24c — Transpose: metrics-as-columns -> stack() -> PIVOT channels to columns
-- @id: Q24c
WITH by_channel AS (
  SELECT
    o.channel,
    CAST(COUNT(DISTINCT o.order_id) AS DOUBLE)                            AS orders,
    CAST(SUM(oi.quantity) AS DOUBLE)                                      AS units,
    CAST(COUNT(DISTINCT o.customer_id) AS DOUBLE)                         AS customers,
    -- stack() needs every value column to have the same type
    CAST(round(AVG(oi.discount_pct) * 100, 2) AS DOUBLE)                  AS avg_disc_pct,
    CAST(round(count_if(o.status IN ('CANCELLED','RETURNED')) / COUNT(*) * 100, 2) AS DOUBLE) AS lost_line_pct
  FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
  GROUP BY o.channel
),
long_fmt AS (
  SELECT channel,
         stack(5, 'orders', orders, 'units', units, 'customers', customers,
                  'avg_disc_pct', avg_disc_pct, 'lost_line_pct', lost_line_pct) AS (metric, value)
  FROM by_channel
)
SELECT *
FROM long_fmt
PIVOT (first(value) FOR channel IN ('web' AS web, 'app' AS app, 'market' AS market))
ORDER BY metric;


-- =====================================================================================
-- Q25 — Spark 4.x-only syntax  (skip these IDs on Spark 3.x)
-- =====================================================================================
-- Q25a — WITH RECURSIVE: hierarchy of any depth (compare with the fixed-depth Q16)
-- @id: Q25a
WITH RECURSIVE tree (node_id, node_name, root_id, depth, path, id_path) AS (
  SELECT node_id, node_name, node_id, 0, node_name, array(node_id)
  FROM category_tree
  WHERE parent_id IS NULL
  UNION ALL
  SELECT c.node_id, c.node_name, t.root_id, t.depth + 1,
         concat(t.path, ' > ', c.node_name), array_append(t.id_path, c.node_id)
  FROM category_tree c
  JOIN tree t ON c.parent_id = t.node_id
  WHERE t.depth < 10                                   -- cycle guard
)
SELECT
  t.node_id,
  t.path,
  t.depth,
  t.id_path,
  size(t.id_path) - 1 = t.depth                       AS depth_consistent,
  (SELECT COUNT(*) FROM tree d WHERE array_contains(d.id_path, t.node_id) AND d.node_id <> t.node_id) AS descendants
FROM tree t
ORDER BY t.path;

-- Q25b — SQL pipe syntax
-- @id: Q25b
FROM orders AS o
|> JOIN order_items AS oi ON oi.order_id = o.order_id
|> WHERE o.status <> 'CANCELLED'
|> EXTEND oi.quantity * oi.unit_price * (1 - oi.discount_pct) AS net_local,
          date_format(o.order_ts, 'yyyy-MM')                   AS ym
|> AGGREGATE COUNT(DISTINCT o.order_id) AS orders, round(SUM(net_local), 2) AS net_local, SUM(oi.quantity) AS units
   GROUP BY o.currency AS currency, ym
|> EXTEND round(net_local / orders, 2) AS avg_order_local
|> WHERE units > 1
|> SELECT currency, ym, orders, units, net_local, avg_order_local
|> ORDER BY currency, ym;

-- Q25c — QUALIFY, LATERAL ... ORDER BY LIMIT, listagg, collations, VARIANT
-- @id: Q25c
WITH agent_stats AS (
  -- UTF8_LCASE collation groups 'PLOY.K@...' and 'ploy.k@...' together
  SELECT trim(agent_email) COLLATE UTF8_LCASE      AS agent,
         COUNT(*)                                  AS tickets,
         listagg(ticket_id, ',') WITHIN GROUP (ORDER BY ticket_id) AS ticket_list
  FROM support_tickets
  WHERE trim(agent_email) <> ''
  GROUP BY trim(agent_email) COLLATE UTF8_LCASE
),
ev AS (
  SELECT
    event_id, customer_id, event_type,
    try_parse_json(payload)                                    AS v,
    variant_get(try_parse_json(payload), '$.device.os', 'string') AS os,
    try_variant_get(try_parse_json(payload), '$.cart_value', 'decimal(12,2)') AS cart_value
  FROM web_events
  WHERE customer_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC, event_id DESC) <= 2
)
SELECT
  c.customer_id,
  c.full_name,
  top_lines.top_order_id,
  top_lines.top_line_local,
  (SELECT listagg(e.event_type, ' / ') WITHIN GROUP (ORDER BY e.event_id)
     FROM ev e WHERE e.customer_id = c.customer_id)           AS last_two_events,
  (SELECT max(e.os) FROM ev e WHERE e.customer_id = c.customer_id)                    AS os_from_variant,
  (SELECT max(e.cart_value) FROM ev e WHERE e.customer_id = c.customer_id)            AS cart_value,
  (SELECT count_if(e.v IS NULL) FROM ev e WHERE e.customer_id = c.customer_id)        AS unparseable_payloads,
  (SELECT max(a.tickets) FROM agent_stats a
    JOIN support_tickets t ON trim(t.agent_email) COLLATE UTF8_LCASE = a.agent
   WHERE t.customer_id = c.customer_id)                                               AS busiest_agent_load
FROM customers c,
LATERAL (
  SELECT oi.order_id AS top_order_id, oi.quantity * oi.unit_price AS top_line_local
  FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
  WHERE o.customer_id = c.customer_id
  ORDER BY oi.quantity * oi.unit_price DESC, oi.order_id, oi.line_no
  LIMIT 1
) top_lines
ORDER BY c.customer_id;


-- =====================================================================================
-- Q26 — THE BIG ONE #2: order margin control tower
--        FIFO unit costs + SCD2 tier at order + category paths + ticket text mining +
--        SLA + FX as-of + hints + DISTRIBUTE BY/SORT BY + multi-window scoring.
--        Cost basis: average FIFO unit cost per product (USD) from inventory
--        consumption; products with no inventory history fall back to 55% of list price.
-- =====================================================================================
-- @id: Q26
WITH
-- 1. FIFO layers and consumption (as in Q14)
inv_layers AS (
  SELECT product_id, warehouse, qty, unit_cost,
         SUM(qty) OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_end
  FROM inventory_movements WHERE move_type IN ('RECEIPT', 'ADJUST_IN')
),
inv_out AS (
  SELECT product_id, warehouse, move_id, qty,
         SUM(qty) OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_end
  FROM inventory_movements WHERE move_type = 'SALE'
),
-- 2. Product FIFO unit cost
fifo_cost AS (
  SELECT
    o.product_id,
    CAST(SUM((least(o.cum_end, l.cum_end) - greatest(o.cum_end - o.qty, l.cum_end - l.qty)) * l.unit_cost)
         / SUM(least(o.cum_end, l.cum_end) - greatest(o.cum_end - o.qty, l.cum_end - l.qty)) AS DECIMAL(12,4)) AS unit_cost_usd
  FROM inv_out o
  JOIN inv_layers l
    ON l.product_id = o.product_id AND l.warehouse = o.warehouse
   AND l.cum_end - l.qty < o.cum_end AND l.cum_end > o.cum_end - o.qty
  GROUP BY o.product_id
),
-- 3. Current stock status per product (any warehouse out of stock?)
stock_now AS (
  SELECT product_id,
         bool_or(on_hand <= 0) AS any_wh_out,
         SUM(on_hand)          AS units_on_hand
  FROM (
    SELECT product_id, warehouse,
           SUM(CASE WHEN move_type IN ('RECEIPT','ADJUST_IN') THEN qty ELSE -qty END) AS on_hand
    FROM inventory_movements
    GROUP BY product_id, warehouse
  )
  GROUP BY product_id
),
-- 4. Category path via fixed-depth walk (as in Q16)
cat_path AS (
  SELECT
    leaf.node_name AS sub_category,
    lower(p1.node_name) AS category,
    concat_ws(' > ', filter(array(p3.node_name, p2.node_name, p1.node_name, leaf.node_name), x -> x IS NOT NULL)) AS path,
    p2.node_name AS division
  FROM category_tree leaf
  LEFT JOIN category_tree p1 ON p1.node_id = leaf.parent_id
  LEFT JOIN category_tree p2 ON p2.node_id = p1.parent_id
  LEFT JOIN category_tree p3 ON p3.node_id = p2.parent_id
  WHERE NOT EXISTS (SELECT 1 FROM category_tree ch WHERE ch.parent_id = leaf.node_id)
),
-- 5. Product dimension with cost basis
prod AS (
  SELECT /*+ BROADCAST(fc), BROADCAST(cp) */
    p.product_id, p.product_name, p.category, p.list_price,
    coalesce(cp.path, 'All Products > Clearance')                         AS cat_path,
    coalesce(cp.division, 'Clearance')                                    AS division,
    coalesce(fc.unit_cost_usd, CAST(p.list_price * 0.55 AS DECIMAL(12,4))) AS unit_cost_usd,
    fc.unit_cost_usd IS NULL                                              AS cost_is_estimate,
    coalesce(sn.any_wh_out, false)                                        AS any_wh_out
  FROM products p
  LEFT JOIN fifo_cost fc ON fc.product_id = p.product_id
  LEFT JOIN cat_path  cp ON cp.sub_category = p.sub_category AND cp.category = p.category
  LEFT JOIN stock_now sn ON sn.product_id = p.product_id
),
-- 6. FX as-of order date
fx AS (
  SELECT order_id, rate_to_usd FROM (
    SELECT o.order_id, f.rate_to_usd,
           ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY f.rate_date DESC) AS rn
    FROM orders o JOIN fx_rates f ON f.currency = o.currency AND f.rate_date <= to_date(o.order_ts)
  ) WHERE rn = 1
),
-- 7. Line-level margin (DISTRIBUTE BY / SORT BY inside a CTE: physical only, results unchanged)
line_margin AS (
  SELECT
    oi.order_id, oi.line_no, oi.product_id, oi.quantity,
    pr.category, pr.division, pr.cat_path, pr.cost_is_estimate, pr.any_wh_out,
    CAST(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * fx.rate_to_usd AS DECIMAL(18,4)) AS net_usd,
    CAST(oi.quantity * pr.unit_cost_usd AS DECIMAL(18,4))                                      AS cogs_usd,
    CAST(oi.quantity * oi.unit_price * oi.discount_pct * fx.rate_to_usd AS DECIMAL(18,4))      AS discount_usd
  FROM order_items oi
  JOIN fx       ON fx.order_id   = oi.order_id
  JOIN prod pr  ON pr.product_id = oi.product_id
  DISTRIBUTE BY oi.order_id
  SORT BY oi.order_id, oi.line_no
),
-- 8. SCD2 tier at order (as in Q15)
scd AS (
  SELECT customer_id, tier, effective_from AS valid_from,
         LEAD(effective_from, 1, DATE'9999-12-31') OVER (PARTITION BY customer_id ORDER BY effective_from) AS valid_to
  FROM (
    SELECT *, LAG(tier) OVER (PARTITION BY customer_id ORDER BY effective_from) AS prev_t,
              ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY effective_from) AS rn
    FROM tier_history
  )
  WHERE rn = 1 OR tier IS DISTINCT FROM prev_t
),
-- 9. Ticket mining: which orders does each ticket mention?
ticket_refs AS (
  SELECT
    t.ticket_id, t.priority, t.csat,
    CAST(ref AS INT)                                                       AS order_id,
    timestampdiff(MINUTE, t.opened_ts, t.first_response_ts)                AS response_min,
    t.resolved_ts IS NULL                                                  AS unresolved,
    lower(t.body) RLIKE 'refund|return|damaged|torn|hot|safety'            AS cost_risk
  FROM support_tickets t
  LATERAL VIEW explode(array_distinct(regexp_extract_all(t.body, '(?i)(?:ord-|order\\s*#?\\s*)(\\d{4})', 1))) r AS ref
),
-- 10. Ticket roll-up per order
order_tickets AS (
  SELECT
    order_id,
    COUNT(*)                                                               AS tickets,
    min(priority)                                                          AS worst_priority,
    bool_or(unresolved)                                                    AS has_open_ticket,
    bool_or(cost_risk)                                                     AS ticket_cost_risk,
    round(avg(csat), 1)                                                    AS avg_csat,
    count_if(response_min IS NULL OR response_min >
             CASE priority WHEN 'P1' THEN 60 WHEN 'P2' THEN 240 WHEN 'P3' THEN 1440 ELSE 2880 END) AS sla_breaches
  FROM ticket_refs
  GROUP BY order_id
),
-- 11. Payment reconciliation (USD)
pay AS (
  SELECT order_id,
         SUM(CASE WHEN payment_status = 'CAPTURED' THEN amount ELSE 0 END) AS captured_local,
         SUM(CASE WHEN payment_status = 'REFUNDED' THEN amount ELSE 0 END) AS refunded_local
  FROM payments GROUP BY order_id
),
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
