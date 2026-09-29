WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id = 37
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    SIZE(COLLECT_SET(status)) AS num_status,
    STDDEV_SAMP(balance) AS balance_stddev
FROM eligible
GROUP BY DATE_TRUNC('YEAR', opened_date)
ORDER BY year_start NULLS LAST;
