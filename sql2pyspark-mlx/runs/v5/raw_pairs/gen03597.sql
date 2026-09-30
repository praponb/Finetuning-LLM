select id, service, log_ts, latency_ms, attr_key, v from app_logs lateral view explode(context) t as attr_key, v
