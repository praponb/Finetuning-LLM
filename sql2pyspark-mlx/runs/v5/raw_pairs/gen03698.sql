SELECT id, service, host, level, attr_key, v FROM app_logs LATERAL VIEW EXPLODE(context) t AS attr_key, v
