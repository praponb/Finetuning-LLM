WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM subscriptions
    WHERE end_date >= DATE '2023-07-13'
      AND plan = 'basic'
)
-- Attach customers attributes
SELECT
    a.id,
    a.customer_id,
    a.start_date,
    b.country,
    b.signup_date,
    b.segment
FROM base a
LEFT JOIN customers b
    ON a.customer_id = b.id
