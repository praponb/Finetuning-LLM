WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('year', delivered_date) AS delivered_date_period,
        COALESCE(cost, 0) AS cost_filled,
        COALESCE(carrier, 'OTHER') AS resolved_carrier
    FROM shipments
    WHERE carrier IN ('Flash', 'DHL')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(cost_filled, cost) AS greatest_cost_filled_cost
    FROM valid
    WHERE shipped_date >= DATE '2023-04-15'
      AND shipped_date <= '2023-11-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost_filled) OVER (ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_filled_avg_5_prior,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY cost ASC, id ASC) AS cost_bucket,
        LAG(greatest_cost_filled_cost, 1) OVER (ORDER BY greatest_cost_filled_cost ASC, id ASC) AS prev_greatest_cost_filled_cost,
        CUME_DIST() OVER w_running AS cost_cume_dist,
        AVG(cost_filled) OVER (PARTITION BY order_id ORDER BY cost_filled ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_filled_avg_6
    FROM cleaned
    WINDOW w_running AS (PARTITION BY status ORDER BY cost DESC NULLS FIRST)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        carrier,
        cost,
        cost_cume_dist,
        prev_greatest_cost_filled_cost
    FROM scored
) src
UNPIVOT (
    value FOR metric IN (cost, cost_cume_dist AS COST_CUME_DIST, prev_greatest_cost_filled_cost AS PREV_GREATEST_COST_FILLED_COST)
)
