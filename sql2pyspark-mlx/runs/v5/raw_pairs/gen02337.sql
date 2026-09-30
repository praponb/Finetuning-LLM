WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        payload_json,
        context,
        context['trace'] AS trace,
        context['version'] AS version,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged
    FROM app_logs
)
-- Ranking and gap features
SELECT
    *,
    LAG(latency_ms, 1) OVER (PARTITION BY level ORDER BY latency_ms, id) AS prev_latency_ms,
    SUM(is_flagged) OVER w_running AS running_is_flagged
FROM unpacked
WINDOW w_running AS (PARTITION BY level, service ORDER BY is_flagged, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
