select id, service, host, status_code, attr_key, attr_value from app_logs lateral view outer explode(context) t as attr_key, attr_value;
