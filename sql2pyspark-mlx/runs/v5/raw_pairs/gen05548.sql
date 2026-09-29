WITH with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        latest.account_type,
        latest.balance
    FROM customers s
    JOIN LATERAL (
            SELECT c.account_type, c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_dense_rank,
        FIRST_VALUE(balance, TRUE) OVER w_hist AS first_known_balance,
        ROW_NUMBER() OVER (ORDER BY balance, id) AS seq_no
    FROM with_latest
    WINDOW w_hist AS (ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(email, 1, 3) AS email_prefix,
        CASE WHEN seq_no >= 7 THEN 'High' WHEN seq_no >= 4 THEN 'Medium' ELSE 'Low' END AS seq_no_band,
        COALESCE(account_type, 'UNKNOWN') AS resolved_account_type
    FROM metrics
    WHERE signup_date >= DATE '2023-08-19'
      AND balance < 30000
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        email,
        is_active,
        account_type,
        balance,
        credit_limit_dense_rank,
        seq_no,
        email_prefix,
        resolved_account_type,
        LEAD(seq_no) OVER w_hist AS next_seq_no,
        LAG(balance) OVER w_hist AS prev_balance,
        LEAD(balance) OVER w_hist AS next_balance,
        ROW_NUMBER() OVER w_hist AS seq_no_2,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM filtered
    WINDOW w_hist AS (PARTITION BY account_type ORDER BY credit_limit, id), w_part AS (PARTITION BY account_type ORDER BY signup_date, id)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND YEAR(x.start_date) = 2025
        )
)
-- Multi-level aggregation
SELECT
    next_seq_no,
    COUNT_IF(account_type = 'SAVINGS') AS savings_events,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    COUNT_IF(account_type = 'CHECKING') AS checking_events
FROM not_flagged
GROUP BY next_seq_no
ORDER BY savings_events DESC NULLS LAST;
