-- Q21a: Per-customer order history as arrays, ~30 array/map functions in one SELECT
-- needs setup views: customers, orders
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
