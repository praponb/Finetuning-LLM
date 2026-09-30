WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY started_at DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS seq_no,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY customer_id, device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country
    FROM sessions
)
-- Aggregate with conditional metrics
SELECT
    country,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN country = 'DE' THEN 1 END), 0), 2) AS seq_no_per_de,
    SUM(CASE WHEN country = 'TH' THEN row_seq ELSE 0 END) AS th_row_seq,
    COUNT(DISTINCT device) AS distinct_devices,
    GROUPING(country) AS gid
FROM metrics
GROUP BY CUBE (country)
ORDER BY distinct_devices DESC
