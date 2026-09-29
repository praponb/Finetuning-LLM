WITH attributed AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM accounts src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date >= sub.start_date
       AND src.opened_date <= DATE_ADD(sub.end_date, 7)
),
scored AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        subscription_id,
        plan,
        monthly_fee,
        AVG(balance) OVER (PARTITION BY account_type, customer_id) AS account_type_avg_balance,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY monthly_fee ASC, id ASC, subscription_id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS balance_avg_3_prior
    FROM attributed
),
with_latest AS (
    SELECT s.*, latest.transaction_amount
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
joined AS (
    SELECT
        s.*,
        c.email,
        c.credit_limit
    FROM with_latest s
    JOIN customers c
        ON s.customer_id = c.id
),
cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        COALESCE(status, 'UNKNOWN') AS status_clean
    FROM joined
    WHERE opened_date >= DATE '2023-05-31'
      AND plan IS NOT NULL
),
cleaned_accounts AS (
    SELECT *, DATE_TRUNC('WEEK', opened_date) AS opened_date_period
    FROM cleaned
    WHERE subscription_id = 5
),
with_dims AS (
    SELECT
        a.*,
        c.country,
        c.name,
        c.signup_date
    FROM cleaned_accounts a
    JOIN customers c
        ON a.customer_id = c.id
)
SELECT
    plan,
    CASE WHEN subscription_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS subscription_id_tier,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count,
    ROUND(AVG(balance), 2) AS avg_balance,
    ROUND(SUM(balance_avg_3_prior) / NULLIF(COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END), 0), 2) AS balance_avg_3_prior_per_loan,
    MAX(opened_date_period) AS last_seen
FROM with_dims
GROUP BY plan, CASE WHEN subscription_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', opened_date)
ORDER BY week_start ASC
