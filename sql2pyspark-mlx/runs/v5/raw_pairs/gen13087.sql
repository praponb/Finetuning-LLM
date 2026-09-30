select id, service, log_ts, status_code, attr_key, value from app_logs lateral view outer explode(context) x as attr_key, value;
