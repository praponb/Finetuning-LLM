WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY opened_date, id) AS latest_rank
    FROM accounts
    QUALIFY latest_rank = 1
)
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    CASE WHEN balance >= 70000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    COUNT_IF(account_type = 'SAVINGS') AS savings_events,
    APPROX_PERCENTILE(balance, 0.5) AS balance_p50
FROM deduped
GROUP BY DATE_TRUNC('WEEK', opened_date), DATE_TRUNC('YEAR', opened_date), CASE WHEN balance >= 70000 THEN 'LARGE' ELSE 'SMALL' END
HAVING APPROX_PERCENTILE(balance, 0.5) > 0
ORDER BY savings_events DESC NULLS LAST
