WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        DATE_TRUNC('year', opened_date) AS opened_date_period,
        CASE WHEN balance >= 86000 THEN 'High' WHEN balance >= 13000 THEN 'Medium' ELSE 'Low' END AS balance_tier
    FROM accounts
    WHERE status = 'FROZEN'
)
-- QUALIFY-style window filter
SELECT
    id,
    customer_id,
    opened_date,
    status
FROM valid
QUALIFY RANK() OVER (PARTITION BY status ORDER BY opened_date_period DESC) = 1;
