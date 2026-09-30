WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        shipped_date,
        cost,
        status,
        FLOOR(cost) AS cost_whole,
        COALESCE(carrier, 'NONE') AS resolved_carrier
    FROM shipments
    WHERE delivered_date > DATE '2023-09-13'
      AND order_id IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        shipped_date,
        status,
        cost_whole,
        resolved_carrier,
        COALESCE(cost_whole, 0) AS cost_whole_filled
    FROM valid
    WHERE shipped_date > DATE '2023-07-29'
      AND cost_whole BETWEEN 27 AND 56
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(cost_whole_filled) OVER w_running AS running_count_cost_whole_filled,
        MAX(cost_whole_filled) OVER w_running AS running_max_cost_whole_filled,
        SUM(cost_whole_filled) OVER w_running AS running_sum_cost_whole_filled
    FROM cleaned
    WINDOW w_running AS (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(cost_whole_filled, 0) AS cost_whole_filled_filled
    FROM scored
    WHERE shipped_date > DATE '2023-06-16'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_sum_cost_whole_filled DESC NULLS LAST, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    shipped_date,
    cost_whole,
    resolved_carrier,
    cost_whole_filled,
    running_count_cost_whole_filled,
    running_max_cost_whole_filled,
    running_sum_cost_whole_filled,
    cost_whole_filled_filled
FROM ranked
ORDER BY cost_whole_filled DESC, cost_whole
