WITH scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS balance_dense_rank,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS LAST) AS balance_dense_rank_2
    FROM accounts
    WHERE account_type = 'SAVINGS'
)
SELECT
    CASE WHEN balance >= 59000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    account_type,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    ROUND(AVG(balance_dense_rank_2), 2) AS avg_balance_dense_rank_2,
    ROUND(AVG(balance_dense_rank_2), 2) AS avg_balance_dense_rank_2_2,
    ROUND(SUM(balance_dense_rank) / NULLIF(COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END), 0), 2) AS balance_dense_rank_per_checking,
    SIZE(COLLECT_SET(status)) AS num_status,
    SUM(CASE WHEN account_type = 'CHECKING' THEN balance_dense_rank ELSE 0 END) AS checking_balance_dense_rank
FROM scored
GROUP BY CASE WHEN balance >= 59000 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', opened_date), account_type
ORDER BY avg_balance_dense_rank_2_2;
