WITH base AS (
    SELECT
        *,
        INITCAP(TRIM(email)) AS email_title,
        CEIL(credit_limit) AS credit_limit_whole
    FROM customers
    WHERE city IS NOT NULL
      AND signup_date >= '2024-07-11'
)
SELECT
    s.id,
    s.name,
    s.email,
    s.city,
    s.segment,
    s.signup_date,
    x.device,
    x.session_country
FROM base s
JOIN LATERAL (
        SELECT c.device, c.country AS session_country
        FROM sessions c
        WHERE c.customer_id = s.id
        ORDER BY c.started_at DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
