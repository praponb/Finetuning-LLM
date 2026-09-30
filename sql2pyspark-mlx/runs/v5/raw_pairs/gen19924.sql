WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        monthly_fee,
        DAYOFWEEK(start_date) start_date_dayofweek,
        DATEDIFF(end_date, start_date) days_start_date_to_end_date
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-05-20')
      AND customer_id = 4
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        plan,
        monthly_fee
    FROM filtered
) src
PIVOT (
    MAX(monthly_fee) AS value_sum,
    COUNT(1) AS item_count
    FOR plan IN ('enterprise', 'premium', 'standard')
)
