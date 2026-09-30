select id, service, log_ts, latency_ms, status_code, attr_key, v from app_logs lateral view outer explode(context) li as attr_key, v;
