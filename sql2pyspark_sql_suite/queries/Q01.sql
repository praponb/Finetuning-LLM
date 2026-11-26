-- Q01: Web sessionization (30-min inactivity gap) + JSON payload parsing
-- needs setup views: web_events
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
