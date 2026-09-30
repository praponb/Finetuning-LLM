with lines as (select id, host, log_ts, level, status_code, attr_key, value from app_logs lateral view explode(context) li as attr_key, value) select id, log_ts, level, attr_key, value from lines;
