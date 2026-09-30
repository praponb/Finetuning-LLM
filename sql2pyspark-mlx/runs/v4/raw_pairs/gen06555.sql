WITH cleaned AS (
    SELECT *, LENGTH(status) AS status_length
    FROM subscriptions
    WHERE end_date >= DATE '2023-07-07'
      AND customer_id IS NOT NULL
)
SELECT
    *,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY status_length DESC) AS status_length_cume_dist,
    CUME_DIST() OVER (PARTITION BY customer_id ORDER BY status_length) AS status_length_cume_dist_2
FROM cleaned
