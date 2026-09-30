WITH lines AS (SELECT id, host, level, latency_ms, k, v FROM app_logs LATERAL VIEW EXPLODE(context) li AS k, v) SELECT id, host, level, k, v FROM lines ORDER BY id NULLS LAST, level;
