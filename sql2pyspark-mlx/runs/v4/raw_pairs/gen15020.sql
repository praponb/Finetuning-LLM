select id, service, log_ts, latency_ms, status_code, key, value from app_logs lateral view outer explode(context) t as key, value;
