select id, service, log_ts, latency_ms, attr_key, value from app_logs lateral view outer explode(context) li as attr_key, value;
