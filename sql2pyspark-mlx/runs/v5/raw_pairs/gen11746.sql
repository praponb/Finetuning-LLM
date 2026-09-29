WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        COALESCE(country, 'Unknown') country_filled,
        UPPER(browser) browser_upper,
        COALESCE(country, 'NONE') country_filled_2
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-08-24 16:00:00'
      AND device = 'mobile'
)
-- Non-equi join on a validity window
SELECT
    src.*,
    sub.id AS subscription_id,
    sub.plan,
    sub.monthly_fee
FROM cleaned src
LEFT JOIN subscriptions sub
    ON src.customer_id = sub.customer_id
   AND src.started_at >= sub.start_date
   AND src.started_at <= DATE_ADD(sub.end_date, 3);
