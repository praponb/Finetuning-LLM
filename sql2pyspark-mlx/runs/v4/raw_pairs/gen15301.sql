WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country, device ORDER BY started_at DESC, id) AS event_seq,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country
    FROM sessions
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.country,
    s.event_seq,
    s.last_known_country,
    (SELECT SUM(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS sum_order_amount
FROM enriched s
ORDER BY customer_id DESC;
