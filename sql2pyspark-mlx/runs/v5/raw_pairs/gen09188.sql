WITH kept_keys AS (
    SELECT plan
    FROM subscriptions
    EXCEPT ALL
    SELECT plan
    FROM subscriptions
    WHERE YEAR(end_date) >= 2025
),
verified AS (
    SELECT s.*
    FROM subscriptions s
    JOIN kept_keys k ON s.plan = k.plan
),
kept_keys_2 AS (
    SELECT plan, id
    FROM verified
    EXCEPT DISTINCT
    SELECT plan, id
    FROM subscriptions
    WHERE MONTH(start_date) IN (2, 7, 11)
),
kept_rows AS (
    SELECT s.*
    FROM verified s
    JOIN kept_keys_2 k ON s.plan = k.plan AND s.id = k.id
),
scored AS (
    SELECT
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        AVG(monthly_fee) OVER (PARTITION BY customer_id, plan) AS customer_id_avg_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share
    FROM kept_rows
),
filtered AS (
    SELECT
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        customer_id_avg_monthly_fee,
        monthly_fee_share,
        customer_id_avg_monthly_fee - monthly_fee_share AS customer_id_avg_monthly_fee_minus_monthly_fee_share,
        TRIM(status) AS clean_status
    FROM scored
    WHERE end_date >= DATE '2023-04-24'
      AND customer_id IS NOT NULL
)
SELECT
    s.customer_id,
    s.start_date,
    s.end_date,
    s.status,
    s.customer_id_avg_monthly_fee,
    s.monthly_fee_share,
    s.customer_id_avg_monthly_fee_minus_monthly_fee_share,
    s.clean_status,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM filtered s
ORDER BY customer_id NULLS LAST, end_date
