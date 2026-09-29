WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, attr_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_code) OVER (PARTITION BY k ORDER BY status_code ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code,
        FIRST_VALUE(k, TRUE) OVER (ORDER BY status_code ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
        ROW_NUMBER() OVER (ORDER BY status_code ASC, id ASC, k ASC) AS row_seq,
        SUM(status_code) OVER (PARTITION BY level ORDER BY status_code ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code_2
    FROM lines
),
with_windows_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY level ORDER BY status_code DESC, id ASC, k ASC) AS status_code_bucket,
        AVG(cumulative_status_code_2) OVER w_part AS host_avg_cumulative_status_code_2
    FROM with_windows
    WHERE row_seq > 5
    WINDOW w_part AS (PARTITION BY host)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(host_avg_cumulative_status_code_2) OVER w_grp AS prev_host_avg_cumulative_status_code_2,
        LAG(status_code_bucket) OVER w_grp AS prev_status_code_bucket,
        ROW_NUMBER() OVER w_grp AS row_seq_2,
        DENSE_RANK() OVER (ORDER BY status_code_bucket ASC NULLS FIRST) AS status_code_bucket_dense_rank
    FROM with_windows_app_logs
    WINDOW w_grp AS (PARTITION BY host, k ORDER BY row_seq ASC, id ASC, k ASC)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        level,
        k,
        attr_value,
        first_known_k,
        row_seq,
        cumulative_status_code_2,
        prev_host_avg_cumulative_status_code_2,
        prev_status_code_bucket,
        status_code_bucket_dense_rank,
        row_seq_2 * status_code_bucket_dense_rank AS row_seq_2_times_status_code_bucket_dense_rank
    FROM enriched
    WHERE status_code >= 265
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_host_avg_cumulative_status_code_2 >= 2137 THEN 'HIGH' ELSE 'LOW' END AS prev_host_avg_cumulative_status_code_2_tier,
    service,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    COUNT(*) AS num_rows,
    COUNT_IF(k = 'version') AS version_events,
    COUNT(CASE WHEN k = 'version' THEN 1 END) AS version_count
FROM cleaned
GROUP BY CUBE (CASE WHEN prev_host_avg_cumulative_status_code_2 >= 2137 THEN 'HIGH' ELSE 'LOW' END, service)
ORDER BY version_count DESC;
