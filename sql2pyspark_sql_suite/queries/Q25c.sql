-- Q25c: QUALIFY, LATERAL ... ORDER BY LIMIT, listagg, collations, VARIANT
-- needs setup views: customers, orders, order_items, web_events, support_tickets
-- requires Spark 4.x
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
