WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_SUB(opened_date, 47) opened_date_minus_47d
    FROM accounts
    WHERE status IS NOT NULL
      AND account_type = 'CREDIT'
),
cleaned_accounts AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        QUARTER(opened_date_minus_47d) opened_date_minus_47d_quarter,
        COALESCE(status, 'UNKNOWN') resolved_status
    FROM cleaned
    WHERE opened_date >= DATE '2023-08-07'
      AND status IS NOT NULL
      AND opened_date_minus_47d BETWEEN '2023-08-01' AND '2024-08-18'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned_accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance,
        LEAD(opened_date_minus_47d_quarter, 2) OVER (PARTITION BY account_type ORDER BY balance, id) next_opened_date_minus_47d_quarter,
        LAST_VALUE(opened_date_minus_47d_quarter, TRUE) OVER (PARTITION BY customer_id ORDER BY opened_date_minus_47d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_opened_date_minus_47d_quarter
    FROM ranked
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_amount
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY status ORDER BY running_balance NULLS FIRST) running_balance_cume_dist,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY transaction_amount) transaction_amount_dense_rank,
        AVG(next_opened_date_minus_47d_quarter) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) next_opened_date_minus_47d_quarter_avg_5_prior
    FROM with_latest
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        account_type,
        next_opened_date_minus_47d_quarter
    FROM scored
) src
PIVOT (
    AVG(next_opened_date_minus_47d_quarter) AS value_sum,
    COUNT(1) AS cnt
    FOR account_type IN ('SAVINGS' AS savings, 'CREDIT' AS credit)
);
