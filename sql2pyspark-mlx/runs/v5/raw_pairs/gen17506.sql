WITH flattened AS (SELECT id, k, value FROM app_logs LATERAL VIEW EXPLODE(context) e AS k, value) SELECT id, k, value FROM flattened ORDER BY k DESC NULLS LAST
