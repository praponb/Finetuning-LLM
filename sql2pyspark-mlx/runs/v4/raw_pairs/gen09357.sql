select id, service, level, latency_ms, key, attr_value from app_logs lateral view explode(context) e as key, attr_value;
