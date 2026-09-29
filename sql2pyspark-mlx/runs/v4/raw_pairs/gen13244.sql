WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(carrier, ' ', status) AS carrier_status,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-04-20')
      AND carrier IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(cost) OVER w_ordered AS status_count_cost,
        CUME_DIST() OVER (ORDER BY cost DESC) AS cost_cume_dist,
        SUM(cost) OVER (PARTITION BY carrier, order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY status)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, carrier ORDER BY running_cost NULLS LAST, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN status_count_cost >= 17 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_count_cost_tier,
        status,
        APPROX_PERCENTILE(status_count_cost, 0.5) AS status_count_cost_p50,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(status_count_cost) AS max_status_count_cost,
        PERCENTILE_APPROX(status_count_cost, ARRAY(0.25, 0.5, 0.75)) AS status_count_cost_quartiles
    FROM ranked
    GROUP BY status_count_cost_tier, status
)
SELECT
    status_count_cost_tier,
    status,
    status_count_cost_p50,
    distinct_ids,
    max_status_count_cost
FROM summary
ORDER BY max_status_count_cost, status_count_cost_tier DESC
