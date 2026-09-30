WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        DAY(delivered_date) delivered_date_day,
        COALESCE(carrier, 'NONE') resolved_carrier
    FROM shipments
    WHERE shipped_date >= DATE '2023-03-21'
      AND carrier IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY shipped_date ASC NULLS FIRST) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id ASC) AS row_num
        FROM ranked
    ) x
    WHERE row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        delivered_date_day,
        resolved_carrier,
        rank_in_group,
        row_num,
        LAG(cost, 1) OVER (PARTITION BY status ORDER BY shipped_date ASC, id ASC) prev_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) cost_share,
        SUM(delivered_date_day) OVER (PARTITION BY status ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_delivered_date_day
    FROM latest
)
SELECT
    id,
    delivered_date,
    cost,
    status,
    delivered_date_day,
    row_num,
    prev_cost,
    cost_share,
    cumulative_delivered_date_day,
    CASE WHEN status RLIKE '^(LOST|IN_TRANSIT)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS status_zone
FROM metrics
ORDER BY status DESC NULLS LAST
