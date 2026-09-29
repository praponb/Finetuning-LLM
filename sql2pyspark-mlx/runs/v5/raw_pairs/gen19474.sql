select id, service, host, log_ts, level, status_code, k, attr_value from app_logs lateral view outer explode(context) li as k, attr_value
