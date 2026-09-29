WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        end_date,
        status,
        UPPER(plan) plan_upper,
        SUBSTRING(status, 1, 2) status_prefix
    FROM subscriptions
    WHERE start_date >= DATE '2023-05-18'
      AND customer_id IN (4, 7, 17)
)
-- QUALIFY-style window filter
SELECT *
FROM valid
QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC NULLS LAST, id) = 1;
