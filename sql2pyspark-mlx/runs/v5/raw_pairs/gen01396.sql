WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label
    FROM accounts
    WHERE account_type IN ('LOAN', 'SAVINGS', 'CREDIT')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date NULLS FIRST, id) = 1
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.opened_date,
        s.opened_date_label,
        top1.txn_time
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, d.credit_limit
    FROM with_latest e
    INNER JOIN customers d
        ON e.customer_id = d.id
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            customer_id,
            account_type,
            balance
        FROM joined
    ) src
    PIVOT (
        MAX(balance) AS amount,
        COUNT(1) AS cnt
        FOR account_type IN ('CHECKING', 'SAVINGS', 'CREDIT')
    )
)
SELECT
    s.customer_id,
    s.CHECKING_amount,
    s.CHECKING_cnt,
    s.SAVINGS_amount,
    s.SAVINGS_cnt,
    s.CREDIT_amount,
    s.CREDIT_cnt,
    (SELECT SUM(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM wide s;
