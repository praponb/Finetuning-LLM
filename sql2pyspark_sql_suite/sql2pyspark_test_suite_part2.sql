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
