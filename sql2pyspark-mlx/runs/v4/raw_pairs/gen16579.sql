WITH lines AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
with_windows AS (
    SELECT
        *,
        CORR(latency_ms, status_code) OVER (PARTITION BY host) AS corr_latency_ms_status_code,
        LAG(latency_ms, 2) OVER (PARTITION BY level ORDER BY status_code, id, k) AS prev_latency_ms
    FROM lines
),
metrics AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        attr_value,
        corr_latency_ms_status_code,
        prev_latency_ms,
        LEAD(status_code, 2) OVER (PARTITION BY service ORDER BY latency_ms, id, k) AS next_status_code,
        ROUND(prev_latency_ms / SUM(prev_latency_ms) OVER (PARTITION BY level, host), 4) AS prev_latency_ms_share,
        AVG(corr_latency_ms_status_code) OVER (PARTITION BY service ORDER BY prev_latency_ms, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS corr_latency_ms_status_code_avg_6_prior,
        CORR(status_code, latency_ms) OVER (PARTITION BY host, level) AS corr_status_code_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY prev_latency_ms DESC, id, k) AS seq_no
    FROM with_windows
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY status_code DESC, id, k) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
metrics_app_logs AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY latest_rank, id, k) AS event_seq,
        SUM(corr_latency_ms_status_code) OVER w_grp AS running_corr_latency_ms_status_code,
        LEAD(corr_latency_ms_status_code_avg_6_prior, 1, 0) OVER (ORDER BY next_status_code, id, k) AS next_corr_latency_ms_status_code_avg_6_prior
    FROM deduped
    WINDOW w_grp AS (PARTITION BY k, service ORDER BY prev_latency_ms_share, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    level,
    AVG(prev_latency_ms) FILTER (WHERE level = 'WARN') AS warn_avg_prev_latency_ms,
    AVG(corr_latency_ms_status_code) FILTER (WHERE host = 'h4') AS h4_avg_corr_latency_ms_status_code,
    BIT_AND(status_code) AS status_code_bit_and
FROM metrics_app_logs
GROUP BY level
HAVING AVG(corr_latency_ms_status_code) FILTER (WHERE host = 'h4') > 0
ORDER BY status_code_bit_and NULLS LAST;
