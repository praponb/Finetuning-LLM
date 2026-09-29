WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        status,
        INITCAP(TRIM(status)) AS status_title,
        CONCAT(status, ' - ', carrier) AS status_carrier,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-04-17')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY status ORDER BY cost) AS cost_cume_dist,
        SUM(cost) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM valid
),
enriched_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        carrier,
        cost,
        status_title,
        carrier_filled,
        cost_cume_dist,
        SUM(cost) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        ROUND(running_cost / SUM(running_cost) OVER (PARTITION BY carrier), 4) AS running_cost_share,
        NTILE(3) OVER (PARTITION BY status ORDER BY cost DESC, id) AS cost_bucket
    FROM enriched
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched_shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cumulative_cost DESC NULLS LAST, id) = 1
)
-- Multi-level aggregation
SELECT
    carrier,
    PERCENTILE_APPROX(cost_bucket, 0.95) AS cost_bucket_p95,
    SIZE(COLLECT_SET(carrier)) AS num_carriers
FROM latest
GROUP BY carrier
HAVING SIZE(COLLECT_SET(carrier)) >= 1;
