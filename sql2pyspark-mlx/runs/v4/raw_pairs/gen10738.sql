WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('YEAR', opened_date) AS opened_date_period,
        LENGTH(account_type) AS account_type_length
    FROM accounts
    WHERE opened_date >= TO_DATE('2024-03-09')
      AND customer_id IN (1, 3, 7)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC NULLS LAST, id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(balance, 1) OVER (PARTITION BY customer_id ORDER BY row_num, id) AS next_balance,
        FIRST_VALUE(balance, TRUE) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
        AVG(account_type_length) OVER (PARTITION BY status) AS status_avg_account_type_length,
        ROW_NUMBER() OVER (ORDER BY row_num, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY account_type ORDER BY account_type_length DESC NULLS LAST) AS account_type_length_cume_dist
    FROM ranked
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY row_seq DESC NULLS FIRST) AS row_seq_dense_rank,
        NTILE(10) OVER (PARTITION BY status ORDER BY status_avg_account_type_length, id) AS status_avg_account_type_length_bucket,
        MAX(account_type_length_cume_dist) OVER w_running AS account_type_length_cume_dist_max_30d,
        COUNT(first_known_balance) OVER w_hist AS status_count_first_known_balance
    FROM enriched
    WINDOW w_running AS (PARTITION BY status ORDER BY CAST(opened_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY status)
)
SELECT
    s.id,
    s.customer_id,
    s.balance,
    s.opened_date,
    s.opened_date_period,
    s.row_num,
    s.next_balance,
    s.first_known_balance,
    s.status_avg_account_type_length,
    (SELECT MAX(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM with_windows s
ORDER BY customer_id DESC NULLS FIRST;
