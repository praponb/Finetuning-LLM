WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) = 1
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date
FROM latest;
