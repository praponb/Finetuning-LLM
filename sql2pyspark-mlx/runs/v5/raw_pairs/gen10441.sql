WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ABS(balance) AS abs_balance
    FROM accounts
    WHERE customer_id = 21
)
-- QUALIFY-style window filter
SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id) = 1;
