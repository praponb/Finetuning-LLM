WITH cleaned AS (
    SELECT
        *,
        SUBSTRING(status, 1, 3) AS status_prefix,
        COALESCE(status, 'NONE') AS resolved_status
    FROM accounts
    WHERE opened_date > TO_DATE('2023-06-03')
      AND customer_id IS NOT NULL
)
SELECT
    *,
    AVG(balance) OVER (PARTITION BY customer_id, account_type ORDER BY balance ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS balance_avg_2_prior,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id ASC) AS row_seq,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY balance ASC NULLS LAST) AS balance_percent_rank,
    AVG(balance) OVER (PARTITION BY account_type) AS account_type_avg_balance
FROM cleaned;
