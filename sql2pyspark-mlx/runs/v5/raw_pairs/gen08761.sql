select id, service, host, level, latency_ms, attr_key, v from app_logs lateral view explode(context) li as attr_key, v
