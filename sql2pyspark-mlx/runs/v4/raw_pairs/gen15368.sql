WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(balance, 1, 0) OVER (ORDER BY opened_date, id) prev_balance,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY balance DESC) balance_dense_rank
    FROM accounts
)
-- Grouped metrics
SELECT
    status,
    CASE WHEN balance_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_dense_rank_tier,
    AVG(prev_balance) FILTER (WHERE status = 'OPEN') AS open_avg_prev_balance,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    COUNT(*) FILTER (WHERE account_type = 'CHECKING') AS checking_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(prev_balance) AS avg_prev_balance,
    GROUPING(status) AS grouping_id
FROM scored
GROUP BY GROUPING SETS ((status, CASE WHEN balance_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END), (status), ())
ORDER BY checking_rows NULLS FIRST;
