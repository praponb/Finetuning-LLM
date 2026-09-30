select id, host, log_ts, level, latency_ms, status_code, k, attr_value from app_logs lateral view outer explode(context) x as k, attr_value;
