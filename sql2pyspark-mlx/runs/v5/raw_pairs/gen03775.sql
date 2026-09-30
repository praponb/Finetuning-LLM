WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(end_date) end_date_month_end,
        DATE_TRUNC('MONTH', start_date) start_date_period
    FROM subscriptions
    WHERE start_date >= DATE '2023-09-27'
      AND customer_id IS NOT NULL
)
-- Attach customers attributes
SELECT
    s.id,
    s.customer_id,
    s.start_date,
    s.status,
    s.end_date_month_end,
    d.age,
    d.name
FROM valid s
LEFT JOIN customers d
    ON s.customer_id = d.id
