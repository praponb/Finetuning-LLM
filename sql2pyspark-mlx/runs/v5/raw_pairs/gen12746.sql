WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(status, ' - ', carrier) status_carrier
    FROM shipments
    WHERE shipped_date > DATE '2023-07-15'
      AND YEAR(delivered_date) >= 2023
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        status_carrier,
        MIN(cost) OVER (PARTITION BY order_id) order_id_min_cost,
        AVG(cost) OVER (PARTITION BY status ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) cost_avg_2,
        ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY cost ASC, id ASC) row_seq,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY cost DESC, id ASC) cost_bucket
    FROM filtered
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY row_seq DESC NULLS FIRST) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
latest_shipments AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost_bucket DESC, id ASC) AS latest_rank
        FROM latest
    ) t
    WHERE latest_rank = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY latest_rank DESC, id ASC) AS dedup_rank
    FROM latest_shipments
    QUALIFY dedup_rank = 1
),
stats AS (
    -- Multi-level aggregation
    SELECT
        status,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(order_id_min_cost, 0.95) AS order_id_min_cost_p95,
        ROUND(SUM(cost_avg_2) / NULLIF(COUNT(CASE WHEN status = 'LOST' THEN 1 END), 0), 2) AS cost_avg_2_per_lost
    FROM deduped
    GROUP BY status
)
SELECT
    status,
    status_list,
    distinct_ids,
    order_id_min_cost_p95,
    CASE WHEN status RLIKE '^(RETURNED|LOST)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS status_zone
FROM stats
