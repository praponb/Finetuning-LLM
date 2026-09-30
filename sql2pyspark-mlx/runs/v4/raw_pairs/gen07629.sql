WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC) balance_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id ASC) row_seq,
        LAG(balance, 1) OVER (PARTITION BY account_type, status ORDER BY balance ASC, id ASC) prev_balance
    FROM accounts
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    SUM(CASE WHEN account_type = 'LOAN' THEN balance_dense_rank ELSE 0 END) AS loan_balance_dense_rank,
    COUNT(*) FILTER (WHERE account_type = 'SAVINGS') AS savings_rows,
    MIN(balance) AS min_balance,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles
FROM with_windows
GROUP BY DATE_TRUNC('WEEK', opened_date)
ORDER BY loan_balance_dense_rank ASC NULLS LAST
