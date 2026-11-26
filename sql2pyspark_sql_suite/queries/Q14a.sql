-- Q14a: COGS per outbound movement
-- needs setup views: products, inventory_movements
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
