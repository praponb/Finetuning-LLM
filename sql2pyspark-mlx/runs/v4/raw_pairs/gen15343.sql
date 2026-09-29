WITH flattened AS (SELECT id, log_ts, level, key, attr_value FROM app_logs LATERAL VIEW EXPLODE(context) t AS key, attr_value) SELECT id, log_ts, level, key, attr_value FROM flattened;
