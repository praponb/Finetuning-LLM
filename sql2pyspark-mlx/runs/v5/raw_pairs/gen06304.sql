WITH exploded AS (
    SELECT
        id,
        host,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
),
cleaned AS (
    SELECT
        *,
        TRIM(host) AS clean_host,
        COALESCE(attr_key, 'N/A') AS resolved_attr_key
    FROM exploded
    WHERE attr_key NOT IN ('trace', 'region')
),
metrics AS (
    SELECT
        id,
        host,
        attr_key,
        LAG(latency_ms, 1) OVER (PARTITION BY attr_key, host ORDER BY latency_ms, id, attr_key) AS prev_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY attr_key, host ORDER BY latency_ms DESC, id, attr_key) AS row_seq
    FROM cleaned
),
enriched AS (
    SELECT
        *,
        ROUND(prev_latency_ms / SUM(prev_latency_ms) OVER (PARTITION BY attr_key), 4) AS prev_latency_ms_share,
        SUM(row_seq) OVER (PARTITION BY host ORDER BY row_seq, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq
    FROM metrics
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY prev_latency_ms_share DESC NULLS FIRST, id, attr_key) AS row_num
    FROM enriched
    QUALIFY row_num = 1
)
SELECT
    CASE WHEN running_row_seq >= 38 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_row_seq_tier,
    CASE WHEN prev_latency_ms_share >= 0.74 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_latency_ms_share_tier,
    CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    COUNT(CASE WHEN attr_key = 'trace' THEN 1 END) AS trace_count,
    COUNT(*) AS num_rows,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    APPROX_PERCENTILE(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    COUNT(*) FILTER (WHERE host = 'h3') AS h3_rows,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys
FROM deduped
GROUP BY CASE WHEN running_row_seq >= 38 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN prev_latency_ms_share >= 0.74 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END
