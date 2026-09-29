SELECT
    id,
    device,
    started_at,
    country,
    NTILE(4) OVER (PARTITION BY device ORDER BY started_at, id) AS started_at_ntile,
    FIRST_VALUE(device) OVER (PARTITION BY country ORDER BY started_at DESC, id) AS first_device
FROM sessions;
