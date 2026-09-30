WITH cleaned AS (
    SELECT
        *,
        CAST(is_active AS INT) AS is_active_flag,
        CAST(credit_limit AS INT) AS credit_limit_int
    FROM customers
    WHERE segment IS NOT NULL
      AND city IS DISTINCT FROM 'Chiang Mai'
)
SELECT s.*
FROM cleaned s
WHERE NOT EXISTS (
        SELECT 1
        FROM sessions ch
        WHERE ch.customer_id = s.id
          AND ch.started_at < s.signup_date
    )
