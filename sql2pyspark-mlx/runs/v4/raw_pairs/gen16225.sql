SELECT
    *,
    FIRST_VALUE(balance, TRUE) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
    RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC NULLS FIRST) AS balance_rank,
    RANK() OVER (ORDER BY balance DESC NULLS FIRST) AS balance_rank_2,
    AVG(balance) OVER (PARTITION BY status, customer_id ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior,
    RANK() OVER (ORDER BY balance NULLS FIRST) AS balance_rank_3
FROM accounts;
