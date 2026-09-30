WITH enriched AS (
    SELECT
        *,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS row_seq
    FROM sessions
)
SELECT
    id,
    customer_id,
    device,
    row_seq
FROM enriched
ORDER BY row_seq DESC NULLS LAST
