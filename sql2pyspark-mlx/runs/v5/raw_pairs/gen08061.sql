WITH exploded AS (SELECT id, service, host, attr_key, v FROM app_logs LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v) SELECT id, host, v FROM exploded ORDER BY host DESC NULLS LAST;
