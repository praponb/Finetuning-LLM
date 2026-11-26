-- Q26 part 1 of 2: THE BIG ONE #2: order margin control tower
-- too long to convert in one go (3,424 tokens), so split by CTE; run parts 1-2 in order
-- needs setup views: customers, products, orders, order_items, payments, fx_rates, inventory_movements, tier_history, category_tree, support_tickets
-- creates temp views: inv_layers, inv_out, fifo_cost, stock_now, cat_path, prod, fx, line_margin, scd, ticket_refs, order_tickets, pay
-- @id: Q26_part1
-- 1. FIFO layers and consumption (as in Q14)
CREATE OR REPLACE TEMP VIEW inv_layers AS
  SELECT product_id, warehouse, qty, unit_cost,
         SUM(qty) OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_end
  FROM inventory_movements WHERE move_type IN ('RECEIPT', 'ADJUST_IN');

CREATE OR REPLACE TEMP VIEW inv_out AS
  SELECT product_id, warehouse, move_id, qty,
         SUM(qty) OVER (PARTITION BY product_id, warehouse ORDER BY move_ts, move_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_end
  FROM inventory_movements WHERE move_type = 'SALE';

-- 2. Product FIFO unit cost
CREATE OR REPLACE TEMP VIEW fifo_cost AS
  SELECT
    o.product_id,
    CAST(SUM((least(o.cum_end, l.cum_end) - greatest(o.cum_end - o.qty, l.cum_end - l.qty)) * l.unit_cost)
         / SUM(least(o.cum_end, l.cum_end) - greatest(o.cum_end - o.qty, l.cum_end - l.qty)) AS DECIMAL(12,4)) AS unit_cost_usd
  FROM inv_out o
  JOIN inv_layers l
    ON l.product_id = o.product_id AND l.warehouse = o.warehouse
   AND l.cum_end - l.qty < o.cum_end AND l.cum_end > o.cum_end - o.qty
  GROUP BY o.product_id;

-- 3. Current stock status per product (any warehouse out of stock?)
CREATE OR REPLACE TEMP VIEW stock_now AS
  SELECT product_id,
         bool_or(on_hand <= 0) AS any_wh_out,
         SUM(on_hand)          AS units_on_hand
  FROM (
    SELECT product_id, warehouse,
           SUM(CASE WHEN move_type IN ('RECEIPT','ADJUST_IN') THEN qty ELSE -qty END) AS on_hand
    FROM inventory_movements
    GROUP BY product_id, warehouse
  )
  GROUP BY product_id;

-- 4. Category path via fixed-depth walk (as in Q16)
CREATE OR REPLACE TEMP VIEW cat_path AS
  SELECT
    leaf.node_name AS sub_category,
    lower(p1.node_name) AS category,
    concat_ws(' > ', filter(array(p3.node_name, p2.node_name, p1.node_name, leaf.node_name), x -> x IS NOT NULL)) AS path,
    p2.node_name AS division
  FROM category_tree leaf
  LEFT JOIN category_tree p1 ON p1.node_id = leaf.parent_id
  LEFT JOIN category_tree p2 ON p2.node_id = p1.parent_id
  LEFT JOIN category_tree p3 ON p3.node_id = p2.parent_id
  WHERE NOT EXISTS (SELECT 1 FROM category_tree ch WHERE ch.parent_id = leaf.node_id);

-- 5. Product dimension with cost basis
CREATE OR REPLACE TEMP VIEW prod AS
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
  LEFT JOIN stock_now sn ON sn.product_id = p.product_id;

-- 6. FX as-of order date
CREATE OR REPLACE TEMP VIEW fx AS
  SELECT order_id, rate_to_usd FROM (
    SELECT o.order_id, f.rate_to_usd,
           ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY f.rate_date DESC) AS rn
    FROM orders o JOIN fx_rates f ON f.currency = o.currency AND f.rate_date <= to_date(o.order_ts)
  ) WHERE rn = 1;

-- 7. Line-level margin (DISTRIBUTE BY / SORT BY inside a CTE: physical only, results unchanged)
CREATE OR REPLACE TEMP VIEW line_margin AS
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
  SORT BY oi.order_id, oi.line_no;

-- 8. SCD2 tier at order (as in Q15)
CREATE OR REPLACE TEMP VIEW scd AS
  SELECT customer_id, tier, effective_from AS valid_from,
         LEAD(effective_from, 1, DATE'9999-12-31') OVER (PARTITION BY customer_id ORDER BY effective_from) AS valid_to
  FROM (
    SELECT *, LAG(tier) OVER (PARTITION BY customer_id ORDER BY effective_from) AS prev_t,
              ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY effective_from) AS rn
    FROM tier_history
  )
  WHERE rn = 1 OR tier IS DISTINCT FROM prev_t;

-- 9. Ticket mining: which orders does each ticket mention?
CREATE OR REPLACE TEMP VIEW ticket_refs AS
  SELECT
    t.ticket_id, t.priority, t.csat,
    CAST(ref AS INT)                                                       AS order_id,
    timestampdiff(MINUTE, t.opened_ts, t.first_response_ts)                AS response_min,
    t.resolved_ts IS NULL                                                  AS unresolved,
    lower(t.body) RLIKE 'refund|return|damaged|torn|hot|safety'            AS cost_risk
  FROM support_tickets t
  LATERAL VIEW explode(array_distinct(regexp_extract_all(t.body, '(?i)(?:ord-|order\\s*#?\\s*)(\\d{4})', 1))) r AS ref;

-- 10. Ticket roll-up per order
CREATE OR REPLACE TEMP VIEW order_tickets AS
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
  GROUP BY order_id;

-- 11. Payment reconciliation (USD)
CREATE OR REPLACE TEMP VIEW pay AS
  SELECT order_id,
         SUM(CASE WHEN payment_status = 'CAPTURED' THEN amount ELSE 0 END) AS captured_local,
         SUM(CASE WHEN payment_status = 'REFUNDED' THEN amount ELSE 0 END) AS refunded_local
  FROM payments GROUP BY order_id;
