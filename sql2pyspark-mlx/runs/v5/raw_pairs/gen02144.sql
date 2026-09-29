WITH cleaned AS (
    SELECT
        *,
        COALESCE(balance, 0) AS balance_filled,
        ROUND(balance * 1.1, 2) AS adjusted_balance
    FROM accounts
    WHERE status IS DISTINCT FROM 'CLOSED'
      AND opened_date >= DATE '2023-10-07'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY adjusted_balance DESC NULLS FIRST, id) AS rn
        FROM cleaned
    ) x
    WHERE rn = 1
),
with_latest AS (
    SELECT
        s.id,
        s.balance_filled,
        s.adjusted_balance,
        s.rn,
        latest.merchant,
        latest.amount
    FROM ranked s
    JOIN LATERAL (
            SELECT c.merchant, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
matched_keys AS (
    SELECT id
    FROM with_latest
    EXCEPT
    SELECT id
    FROM accounts
    WHERE customer_id = 30
),
verified AS (
    SELECT s.*
    FROM with_latest s
    JOIN matched_keys k ON s.id = k.id
),
cleaned_accounts AS (
    SELECT *, rn - amount AS rn_minus_amount
    FROM verified
    WHERE merchant IS NOT DISTINCT FROM 'Shell'
),
base AS (
    SELECT
        *,
        rn - adjusted_balance AS rn_minus_adjusted_balance,
        amount - adjusted_balance AS amount_minus_adjusted_balance,
        COALESCE(merchant, 'NONE') AS merchant_clean
    FROM cleaned_accounts
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM '7-Eleven'
)
SELECT
    CASE WHEN amount_minus_adjusted_balance >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_minus_adjusted_balance_tier,
    CASE WHEN balance_filled >= 67 THEN 'HIGH' ELSE 'LOW' END AS balance_filled_tier,
    COUNT(*) FILTER (WHERE merchant = '7-Eleven') AS 7_eleven_rows,
    APPROX_PERCENTILE(amount_minus_adjusted_balance, 0.9) AS amount_minus_adjusted_balance_p90,
    COUNT(CASE WHEN merchant = 'Shell' THEN 1 END) AS shell_count
FROM base
GROUP BY ROLLUP (CASE WHEN amount_minus_adjusted_balance >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN balance_filled >= 67 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY balance_filled_tier DESC;
