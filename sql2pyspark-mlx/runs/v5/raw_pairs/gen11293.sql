select id, log_ts, level, key, attr_value from app_logs lateral view explode(context) x as key, attr_value;
