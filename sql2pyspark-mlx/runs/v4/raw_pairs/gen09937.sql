select id, service, host, level, latency_ms, key, value from app_logs lateral view outer explode(context) t as key, value
