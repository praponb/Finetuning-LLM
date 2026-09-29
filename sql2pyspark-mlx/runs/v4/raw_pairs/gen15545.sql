WITH scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY started_at DESC, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY started_at DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS seq_no
    FROM sessions
)
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    APPROX_PERCENTILE(seq_no, 0.9) AS seq_no_p90,
    COUNT_IF(country = 'DE') AS de_events,
    MAX(started_at) AS last_seen
FROM scored
GROUP BY 1
HAVING de_events < 1
ORDER BY year_start DESC NULLS LAST;
