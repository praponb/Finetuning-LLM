-- Q05a: Arrays, maps, structs and higher-order functions; LATERAL VIEW explode
-- needs setup views: customers, products, orders
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
