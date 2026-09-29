select id, service, host, latency_ms, status_code, attr_key, v from app_logs lateral view explode(context) x as attr_key, v
