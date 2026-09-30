WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        LAST_VALUE(status, TRUE) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status,
        LAST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance,
        MAX(balance) OVER w_part AS customer_id_max_balance
    FROM accounts
    WINDOW w_part AS (PARTITION BY customer_id)
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.opened_date,
    s.status,
    s.last_known_status,
    CASE WHEN account_type RLIKE '^(CREDIT|CHECKING)$' THEN 'CORE' WHEN account_type RLIKE '^(SAVINGS|LOAN)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS account_type_zone,
    (SELECT MIN(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM enriched s
