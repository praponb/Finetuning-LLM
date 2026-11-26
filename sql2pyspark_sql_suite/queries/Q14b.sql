-- Q14b: Ending inventory valuation (remaining FIFO layers) with ROLLUP
-- needs setup views: products, inventory_movements
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
