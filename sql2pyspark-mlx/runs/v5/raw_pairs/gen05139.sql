WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC NULLS LAST, id) AS rank_in_group
    FROM sessions
    QUALIFY rank_in_group = 1
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.country,
    CASE WHEN country RLIKE '^(DE|US)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS country_zone,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM ranked s;
