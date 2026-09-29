SELECT
    id,
    customer_id,
    balance,
    opened_date,
    PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY balance) AS balance_pct_rank,
    RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS opened_date_rank
FROM accounts;
