SELECT id, log_ts, latency_ms, status_code, payload_json, GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id, CAST((status_code & 1) != 0 AS INT) AS flag_bit1 FROM app_logs;
