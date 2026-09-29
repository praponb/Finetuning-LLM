select id, host, level, k, value from app_logs lateral view explode(context) e as k, value
