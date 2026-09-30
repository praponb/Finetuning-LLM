WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        LAST_DAY(delivered_date) AS delivered_date_month_end,
        CONCAT_WS('/', status, carrier) AS status_carrier_key
    FROM shipments
    WHERE carrier IS NOT NULL
      AND carrier IS NOT DISTINCT FROM 'Kerry'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        cost,
        delivered_date_month_end,
        status_carrier_key,
        RANK() OVER (PARTITION BY order_id ORDER BY cost ASC) AS cost_rank,
        FIRST_VALUE(carrier) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier,
        LAG(cost, 1) OVER (PARTITION BY carrier ORDER BY cost ASC, id ASC) AS prev_cost
    FROM cleaned
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(cost_rank, 1) OVER (ORDER BY cost ASC, id ASC) AS next_cost_rank,
        NTILE(5) OVER (ORDER BY cost ASC, id ASC) AS cost_bucket
    FROM with_windows
    WHERE prev_cost <= 47
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(prev_cost AS INT) AS prev_cost_int
    FROM scored
    WHERE delivered_date_month_end <= '2024-02-01'
)
SELECT
    id,
    delivered_date_month_end,
    cost_rank,
    first_known_carrier,
    cost_bucket,
    prev_cost_int
FROM valid
ORDER BY cost_rank DESC
