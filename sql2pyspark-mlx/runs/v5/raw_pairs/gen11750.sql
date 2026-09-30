WITH valid AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        UPPER(account_type) account_type_upper,
        INITCAP(TRIM(account_type)) account_type_title
    FROM accounts
    WHERE opened_date > DATE '2023-05-13'
      AND customer_id IS NOT NULL
),
scored AS (
    SELECT
        *,
        AVG(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) balance_avg_5_prior,
        AVG(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) balance_avg_6_prior
    FROM valid
    WHERE MONTH(opened_date) IN (6, 11, 12)
),
valid_accounts AS (
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        account_type_upper,
        LOWER(account_type) account_type_lower,
        COALESCE(status, 'UNKNOWN') resolved_status
    FROM scored
    WHERE status IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM valid_accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
)
SELECT
    dim.country,
    e.status,
    COUNT(*) AS rows_total,
    ROUND(SUM(e.balance), 2) AS total_balance
FROM kept e
LEFT JOIN customers dim ON e.customer_id = dim.id
GROUP BY dim.country, e.status
ORDER BY status DESC NULLS LAST;
