WITH base AS (
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END AS is_returned
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-07-02')
      AND carrier IS DISTINCT FROM 'Flash'
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY is_returned ASC, id ASC) AS dedup_rank
        FROM base
    ) x
    WHERE dedup_rank = 1
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date ASC NULLS FIRST, id ASC) AS row_num
    FROM latest
    QUALIFY row_num = 1
),
valid AS (
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        is_returned,
        dedup_rank,
        row_num,
        CAST(dedup_rank AS DOUBLE) AS dedup_rank_str,
        row_num + cost AS row_num_plus_cost,
        COALESCE(carrier, 'N/A') AS resolved_carrier
    FROM deduped
    WHERE carrier IS NOT NULL
      AND carrier IS DISTINCT FROM 'Flash'
),
with_windows AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY carrier ORDER BY is_returned DESC, id ASC) AS is_returned_bucket,
        FIRST_VALUE(dedup_rank_str, TRUE) OVER (PARTITION BY carrier ORDER BY dedup_rank_str ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dedup_rank_str,
        AVG(row_num_plus_cost) OVER (PARTITION BY carrier ORDER BY shipped_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS row_num_plus_cost_avg_5
    FROM valid
)
SELECT
    CASE WHEN row_num_plus_cost >= 79 THEN 'HIGH' ELSE 'LOW' END AS row_num_plus_cost_tier,
    DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    ROUND(SUM(dedup_rank_str) / NULLIF(COUNT(CASE WHEN carrier = 'UPS' THEN 1 END), 0), 2) AS dedup_rank_str_per_ups,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN row_num_plus_cost >= 79 THEN 'HIGH' ELSE 'LOW' END ORDER BY ROUND(SUM(dedup_rank_str) / NULLIF(COUNT(CASE WHEN carrier = 'UPS' THEN 1 END), 0), 2) DESC) AS dedup_rank_str_per_ups_dense_rank
FROM with_windows
GROUP BY 1, 2
ORDER BY num_carriers ASC;
