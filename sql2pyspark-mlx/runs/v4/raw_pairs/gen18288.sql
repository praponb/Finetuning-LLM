select id, service, log_ts, level, latency_ms, status_code, k, attr_value from app_logs lateral view explode(context) e as k, attr_value;
