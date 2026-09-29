WITH flattened AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
),
unpacked AS (
    SELECT
        id,
        host,
        status_code,
        key,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE host <> 'h3'
),
filtered AS (
    SELECT
        *,
        status_code_high + status_code AS status_code_high_plus_status_code,
        COALESCE(key, 'OTHER') AS resolved_key
    FROM unpacked
    WHERE key IN ('version', 'trace')
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host, key ORDER BY status_code DESC, id, key) AS dedup_rank
        FROM filtered
    ) t
    WHERE dedup_rank = 1
),
scored AS (
    SELECT
        id,
        host,
        status_code,
        key,
        status_code_high_plus_status_code,
        dedup_rank,
        RANK() OVER (PARTITION BY key ORDER BY status_code DESC) AS status_code_rank,
        ROW_NUMBER() OVER w_grp AS event_seq,
        AVG(status_code_high) OVER (ORDER BY status_code_high, id, key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS status_code_high_avg_6,
        AVG(status_code) OVER w_part AS status_code_avg_6
    FROM ranked
    WHERE status_code_high <> 1
    WINDOW w_grp AS (PARTITION BY key ORDER BY dedup_rank DESC, id, key), w_part AS (PARTITION BY key ORDER BY dedup_rank, id, key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN status_code_high_avg_6 >= 2 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_avg_6_tier,
    host,
    ROUND(SUM(status_code_high_avg_6) / NULLIF(COUNT(CASE WHEN host = 'h2' THEN 1 END), 0), 2) AS status_code_high_avg_6_per_h2,
    BIT_OR(status_code) AS status_code_bit_or
FROM scored
GROUP BY GROUPING SETS ((CASE WHEN status_code_high_avg_6 >= 2 THEN 'HIGH' ELSE 'LOW' END, host), (CASE WHEN status_code_high_avg_6 >= 2 THEN 'HIGH' ELSE 'LOW' END), ());
