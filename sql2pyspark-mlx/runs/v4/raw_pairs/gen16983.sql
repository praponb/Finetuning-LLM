SELECT
    *,
    LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
    ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS seq_no
FROM sessions;
