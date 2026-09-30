select id, host, level, latency_ms, status_code, k, attr_value from app_logs lateral view outer explode(context) t as k, attr_value
