SELECT id, level, status_code, attr_key, attr_value FROM app_logs LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value;
