-- Q13a: Stock-state islands per product/warehouse (IN_STOCK / STOCKOUT / BACKORDER)
-- needs setup views: inventory_movements
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
