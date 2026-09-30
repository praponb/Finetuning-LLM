WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY cost DESC, id ASC) AS cost_bucket,
        MIN(cost) OVER (PARTITION BY carrier) AS carrier_min_cost
    FROM shipments
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY carrier_min_cost DESC NULLS LAST, id ASC) AS dedup_rank
        FROM metrics
    ) ranked
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, p.status AS order_status
    FROM deduped e
    INNER JOIN orders p
        ON e.order_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        shipped_date,
        delivered_date,
        status,
        cost_bucket,
        order_status,
        SUM(carrier_min_cost) OVER (PARTITION BY status, order_id ORDER BY cost_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_carrier_min_cost,
        LAST_VALUE(order_status, TRUE) OVER (PARTITION BY status, order_id ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_order_status
    FROM joined
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LAST_DAY(delivered_date) AS delivered_date_month_end,
        COALESCE(order_status, 'N/A') AS resolved_order_status
    FROM scored
    WHERE order_status IS NOT NULL
      AND order_status IS DISTINCT FROM 'RETURNED'
)
SELECT
    id,
    shipped_date,
    delivered_date,
    status,
    running_carrier_min_cost,
    last_known_order_status,
    delivered_date_month_end,
    resolved_order_status,
    CASE WHEN status RLIKE '^(DELIVERED|IN_TRANSIT)$' THEN 'GROUP_A' WHEN status RLIKE '^(LOST|RETURNED)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS status_zone
FROM valid
ORDER BY delivered_date_month_end DESC NULLS LAST, shipped_date ASC
