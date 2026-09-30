WITH valid AS (
    SELECT
        *,
        TRIM(account_type) AS clean_account_type,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN', 'CLOSED')
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.opened_date
        )
),
joined AS (
    SELECT src.*, p.credit_limit
    FROM eligible src
    JOIN customers p
        ON src.customer_id = p.id
),
enriched AS (
    SELECT e.*, c.city
    FROM joined e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
eligible_accounts AS (
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND TO_DATE(c.txn_time) >= '2024-03-01'
        )
),
with_last AS (
    SELECT
        s.*,
        x.transaction_amount,
        x.transaction_txn_type
    FROM eligible_accounts s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
attributed AS (
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_last s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.opened_date >= sub.start_date
       AND s.opened_date <= DATE_ADD(sub.end_date, 10)
)
SELECT *
FROM (
    SELECT
        id,
        subscription_id,
        status,
        transaction_amount,
        credit_limit,
        monthly_fee
    FROM attributed
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (transaction_amount AS TRANSACTION_AMOUNT_VAL, credit_limit AS CREDIT_LIMIT, monthly_fee AS MONTHLY_FEE)
);
