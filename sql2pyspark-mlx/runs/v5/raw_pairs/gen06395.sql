WITH base AS (
    SELECT
        id,
        cost,
        status,
        CONCAT_WS('/', carrier, status) carrier_status_key
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'Kerry'
      AND shipped_date < '2024-05-01'
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) AS latest_rank
        FROM base
    ) ranked
    WHERE latest_rank = 1
),
latest_shipments AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY latest_rank DESC, id) AS rank_in_group
        FROM latest
    ) ranked
    WHERE rank_in_group = 1
),
base_shipments AS (
    SELECT
        id,
        cost,
        status,
        latest_rank,
        rank_in_group,
        LEAST(latest_rank, rank_in_group) least_latest_rank_rank_in_group
    FROM latest_shipments
    WHERE status = 'LOST'
),
metrics AS (
    SELECT
        id,
        status,
        latest_rank,
        rank_in_group,
        AVG(cost) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_cost,
        MAX(cost) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_cost,
        COUNT(cost) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_cost
    FROM base_shipments
)
SELECT
    status,
    CASE WHEN running_count_cost >= 15 THEN 'HIGH' ELSE 'LOW' END AS running_count_cost_tier,
    COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count,
    COUNT(*) FILTER (WHERE status = 'RETURNED') AS returned_rows,
    MIN(running_avg_cost) AS min_running_avg_cost
FROM metrics
GROUP BY CUBE (status, CASE WHEN running_count_cost >= 15 THEN 'HIGH' ELSE 'LOW' END)
HAVING delivered_count < 1000
ORDER BY delivered_count DESC;
