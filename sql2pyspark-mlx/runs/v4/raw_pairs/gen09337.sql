WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY carrier, order_id ORDER BY cost DESC NULLS FIRST) AS cost_percent_rank,
        LAG(cost, 2) OVER (PARTITION BY status ORDER BY shipped_date, id) AS prev_cost,
        SUM(cost) OVER (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM shipments
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cumulative_cost DESC NULLS LAST, id) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(cost_percent_rank, 0) AS cost_percent_rank_filled,
        CAST(prev_cost AS INT) AS prev_cost_int
    FROM ranked
    WHERE carrier IS NOT NULL
      AND running_cost BETWEEN 158 AND 474
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY prev_cost_int, id) AS rn
        FROM base
    ) t
    WHERE rn = 1
),
scored_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY order_id ORDER BY prev_cost NULLS LAST) AS prev_cost_rank,
        LAST_VALUE(cost_percent_rank_filled, TRUE) OVER (ORDER BY cost_percent_rank_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost_percent_rank_filled,
        LAST_VALUE(cost_percent_rank_filled) IGNORE NULLS OVER (ORDER BY cumulative_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost_percent_rank_filled_2
    FROM latest
)
SELECT
    s.id,
    s.order_id,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.prev_cost,
    s.latest_rank,
    s.rn,
    s.prev_cost_rank,
    (SELECT SUM(x.unit_price) FROM order_items x WHERE x.order_id = s.order_id) AS sum_order_item_unit_price
FROM scored_shipments s
ORDER BY prev_cost DESC, cost
