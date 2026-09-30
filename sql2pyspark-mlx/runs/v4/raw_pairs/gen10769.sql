SELECT id, service, payload_json, context, SHIFTRIGHT(status_code, 2) AS status_code_high, GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier FROM app_logs
